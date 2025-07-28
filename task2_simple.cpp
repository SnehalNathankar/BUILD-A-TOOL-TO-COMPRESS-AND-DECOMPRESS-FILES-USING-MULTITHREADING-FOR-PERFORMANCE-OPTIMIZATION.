#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>

constexpr size_t CHUNK_SIZE = 1024 * 1024;

struct Chunk {
    size_t index;
    std::vector<char> data;
};

std::queue<Chunk> workQueue;
std::mutex queueMutex, outputMutex;
std::condition_variable cv;
bool doneReading = false;

std::vector<char> RLECompress(const std::vector<char>& input) {
    std::vector<char> output;
    size_t i = 0;
    while (i < input.size()) {
        char current = input[i];
        size_t count = 1;
        while (i + count < input.size() && input[i + count] == current && count < 255) {
            ++count;
        }
        output.push_back(current);
        output.push_back(static_cast<char>(count));
        i += count;
    }
    return output;
}

std::vector<char> RLEDecompress(const std::vector<char>& input) {
    std::vector<char> output;
    for (size_t i = 0; i + 1 < input.size(); i += 2) {
        char ch = input[i];
        unsigned char count = static_cast<unsigned char>(input[i + 1]);
        output.insert(output.end(), count, ch);
    }
    return output;
}

void compressWorker(const std::string& outputFile) {
    std::ofstream out(outputFile, std::ios::binary | std::ios::app);
    while (true) {
        Chunk chunk;

        {
            std::unique_lock<std::mutex> lock(queueMutex);
            cv.wait(lock, [] { return !workQueue.empty() || doneReading; });
            if (workQueue.empty()) break;
            chunk = workQueue.front();
            workQueue.pop();
        }

        std::vector<char> compressed = RLECompress(chunk.data);
        {
            std::lock_guard<std::mutex> lock(outputMutex);
            size_t size = compressed.size();
            out.write(reinterpret_cast<char*>(&chunk.index), sizeof(size_t));
            out.write(reinterpret_cast<char*>(&size), sizeof(size_t));
            out.write(compressed.data(), size);
        }
    }
    out.close();
}

void compressFile(const std::string& inputFile, const std::string& outputFile, int threadCount) {
    std::ifstream in(inputFile, std::ios::binary);
    std::vector<std::thread> threads;
    size_t index = 0;

    for (int i = 0; i < threadCount; ++i)
        threads.emplace_back(compressWorker, outputFile);

    while (!in.eof()) {
        std::vector<char> buffer(CHUNK_SIZE);
        in.read(buffer.data(), CHUNK_SIZE);
        size_t bytesRead = in.gcount();
        if (bytesRead == 0) break;
        buffer.resize(bytesRead);

        {
            std::lock_guard<std::mutex> lock(queueMutex);
            workQueue.push({index++, buffer});
        }
        cv.notify_one();
    }

    in.close();

    {
        std::lock_guard<std::mutex> lock(queueMutex);
        doneReading = true;
    }
    cv.notify_all();

    for (auto& t : threads) t.join();
}

void decompressFile(const std::string& inputFile, const std::string& outputFile) {
    std::ifstream in(inputFile, std::ios::binary);
    std::ofstream out(outputFile, std::ios::binary);

    while (in.peek() != EOF) {
        size_t index = 0, size = 0;
        in.read(reinterpret_cast<char*>(&index), sizeof(size_t));
        in.read(reinterpret_cast<char*>(&size), sizeof(size_t));

        std::vector<char> compressed(size);
        in.read(compressed.data(), size);

        std::vector<char> decompressed = RLEDecompress(compressed);
        out.write(decompressed.data(), decompressed.size());
    }

    in.close();
    out.close();
}

int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cerr << "Usage: " << argv[0] << " <compress|decompress> <input> <output> <threads>\n";
        return 1;
    }

    std::string mode = argv[1];
    std::string input = argv[2];
    std::string output = argv[3];
    int threads = std::stoi(argv[4]);

    if (mode == "compress") {
        compressFile(input, output, threads);
        std::cout << "Compression complete.\n";
    } else if (mode == "decompress") {
        decompressFile(input, output);
        std::cout << "Decompression complete.\n";
    } else {
        std::cerr << "Invalid mode: use 'compress' or 'decompress'\n";
        return 1;
    }

    return 0;
}
