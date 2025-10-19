#include <iostream>
#include <string>
#include "async.h"

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "bulk <N>\n";
        return 1;
    }

    std::size_t bulk = 0;
    try {
        bulk = static_cast<std::size_t>(std::stoul(argv[1]));
    } catch (...) {
        return 1;
    }
    if (bulk == 0) {
        std::cerr << "N <= 0\n";
        return 1;
    }

    auto h = async::connect(bulk);
    if (!h) {
        return 2;
    }

    std::string line;
    while (std::getline(std::cin, line)) {
        line.push_back('\n');
        async::receive(h, line.c_str(), line.size());
    }

    async::disconnect(h);
    return 0;
}