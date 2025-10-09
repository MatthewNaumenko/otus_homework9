#include <iostream>
#include <thread>
#include <vector>
#include "async.h"

int main(int, char *[]) {
    const std::size_t bulk = 5;

    auto h1 = async::connect(bulk);
    auto h2 = async::connect(bulk);
    auto h3 = async::connect(3); 

    // поток 1 -> h1
    std::thread t1([&]{
        async::receive(h1, "1\n2\n3\n4\n", 8);
        async::receive(h1, "5\n6\n{\na\nb\nc\n}\n89\n", 17);
    });

    // поток 2 -> h2
    std::thread t2([&]{
        async::receive(h2, "x\n", 2);
        async::receive(h2, "y\nz\n", 4);
        async::receive(h2, "{\nA\nB\n", 6);
        async::receive(h2, "C\n}\n", 4);
        async::receive(h2, "Q\n", 2);
    });

    // поток 3 -> h3
    std::thread t3([&]{
        async::receive(h3, "m\nn\n", 4);
        async::receive(h3, "o\np\nq\n", 6);
        // незавершённый динамический блок будет проигнорирован при disconnect
        async::receive(h3, "{\n1\n2\n", 6);
    });

    t1.join();
    t2.join();
    t3.join();

    async::disconnect(h1);
    async::disconnect(h2);
    async::disconnect(h3);

    return 0;
}

