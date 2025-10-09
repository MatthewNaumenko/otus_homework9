#include "async.h"  // внешний API (не меняем)
#include <atomic>
#include <condition_variable>
#include <ctime>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace async {

struct Bulk {
    std::vector<std::string> cmds;
    std::time_t first_ts = 0;
};

template<typename T>
class BlockingQueue {
public:
    void push(T v) {
        {
            std::lock_guard<std::mutex> lk(mx_);
            q_.push_back(std::move(v));
        }
        cv_.notify_one();
    }

    bool pop(T& out) {
        std::unique_lock<std::mutex> lk(mx_);
        cv_.wait(lk, [&]{ return stop_ || !q_.empty(); });
        if (stop_ && q_.empty()) return false;
        out = std::move(q_.front());
        q_.pop_front();
        return true;
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lk(mx_);
            stop_ = true;
        }
        cv_.notify_all();
    }

private:
    std::mutex mx_;
    std::condition_variable cv_;
    std::deque<T> q_;
    bool stop_ = false;
};

class Dispatcher {
public:
    static Dispatcher& instance() {
        static Dispatcher d;
        return d;
    }

    void post_bulk(const std::shared_ptr<Bulk>& b) {
        log_q_.push(b);
        file_q_.push(b);
    }

    // явная остановка потоков
    void shutdown() {
        bool expected = false;
        if (!stopped_.compare_exchange_strong(expected, true)) return; // уже остановлены
        log_q_.stop();
        file_q_.stop();
        if (log_th_.joinable())   log_th_.join();
        if (file_th_[0].joinable()) file_th_[0].join();
        if (file_th_[1].joinable()) file_th_[1].join();
    }

private:
    Dispatcher() {
        // лог-поток
        log_th_ = std::thread([this]{
            std::shared_ptr<Bulk> b;
            while (log_q_.pop(b)) {
                if (!b || b->cmds.empty()) continue;
                std::ostringstream os;
                os << "bulk: ";
                for (size_t i = 0; i < b->cmds.size(); ++i) {
                    if (i) os << ", ";
                    os << b->cmds[i];
                }
                os << '\n';
                std::cout << os.str();
            }
        });

        // два файловых потока общая очередь
        file_th_[0] = std::thread([this]{ file_worker(1); });
        file_th_[1] = std::thread([this]{ file_worker(2); });
    }

    ~Dispatcher() {
        shutdown();
    }

    void file_worker(int worker_id) {
        // всегда пишем в ./logs
        const std::filesystem::path out_dir = std::filesystem::path("logs");
        std::error_code ec;
        std::filesystem::create_directories(out_dir, ec); 

        std::shared_ptr<Bulk> b;
        static std::atomic<unsigned long long> seq{0}; // общий счётчик
        while (file_q_.pop(b)) {
            if (!b || b->cmds.empty()) continue;

            const auto local_seq = ++seq;

            std::ostringstream fname;
            fname << "bulk" << static_cast<long long>(b->first_ts)
                  << "_" << local_seq
                  << "_t" << worker_id
                  << ".log";

            const auto path = out_dir / fname.str();

            std::ofstream ofs(path, std::ios::out | std::ios::trunc | std::ios::binary);
            if (!ofs) {
                std::cerr << "[file" << worker_id << "] can't open " << path.string() << "\n";
                continue;
            }

            ofs << "bulk: ";
            for (size_t i = 0; i < b->cmds.size(); ++i) {
                if (i) ofs << ", ";
                ofs << b->cmds[i];
            }
            ofs << '\n';

            ofs.flush();
            ofs.close();
        }
    }

private:
    BlockingQueue<std::shared_ptr<Bulk>> log_q_;
    BlockingQueue<std::shared_ptr<Bulk>> file_q_;
    std::thread log_th_;
    std::thread file_th_[2];
    std::atomic<bool> stopped_{false};
};

struct DispatcherInit {
    DispatcherInit()  { (void)Dispatcher::instance(); }
    ~DispatcherInit() = default;
};
static DispatcherInit g_dispatcher_init;

struct Context {
    explicit Context(std::size_t n) : N(n) {}

    void on_line(const std::string& line) {
        if (line == "{") { on_open(); return; }
        if (line == "}") { on_close(); return; }
        if (!line.empty()) on_cmd(line);
    }

    void on_eof() {
        if (!partial_.empty() && depth == 0) {
            on_line(partial_);
        }
        partial_.clear();

        if (depth == 0) {
            flush_if_needed();
        } else {
            // внутри динамического блока всё игнорирутся
            buf.clear();
            first_ts = 0;
            depth = 0;
        }
    }

    void on_data(const char* data, std::size_t size) {
        for (std::size_t i = 0; i < size; ++i) {
            char c = data[i];
            if (c == '\n') {
                on_line(partial_);
                partial_.clear();
            } else {
                partial_.push_back(c);
            }
        }
    }

private:
    void on_open() {
        if (depth == 0) flush_if_needed();
        ++depth;
    }

    void on_close() {
        if (depth == 0) return; 
        --depth;
        if (depth == 0) flush_if_needed();
    }

    void on_cmd(const std::string& cmd) {
        if (buf.empty()) first_ts = std::time(nullptr);
        buf.push_back(cmd);
        if (depth == 0 && buf.size() == N) flush();
    }

    void flush_if_needed() {
        if (!buf.empty()) flush();
    }

    void flush() {
        auto b = std::make_shared<Bulk>();
        b->cmds = std::move(buf);
        b->first_ts = first_ts;

        Dispatcher::instance().post_bulk(b);

        buf.clear();
        first_ts = 0;
    }

public:
    std::mutex mx;
    const std::size_t N;
    std::vector<std::string> buf;
    std::string partial_;
    std::time_t first_ts = 0;
    int depth = 0;
};

// счётчик активных контекстов
static std::atomic<int> g_active_contexts{0};

handle_t connect(std::size_t bulk) {
    try {
        if (bulk == 0) return nullptr;
        (void)Dispatcher::instance(); 
        g_active_contexts.fetch_add(1, std::memory_order_relaxed);
        return new Context(bulk);
    } catch (...) {
        return nullptr;
    }
}

void receive(handle_t handle, const char *data, std::size_t size) {
    if (!handle || !data || size == 0) return;
    auto* ctx = static_cast<Context*>(handle);
    std::lock_guard<std::mutex> lk(ctx->mx);
    ctx->on_data(data, size);
}

void disconnect(handle_t handle) {
    if (!handle) return;
    std::unique_ptr<Context> ctx(static_cast<Context*>(handle));
    {
        std::lock_guard<std::mutex> lk(ctx->mx);
        ctx->on_eof();
    }
    // если это был последний контекст корректно останавливаем диспетчер
    if (g_active_contexts.fetch_sub(1, std::memory_order_relaxed) == 1) {
        Dispatcher::instance().shutdown();
    }
}

} // namespace async
