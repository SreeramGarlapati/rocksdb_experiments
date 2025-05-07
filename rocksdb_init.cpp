#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <chrono>
#include <thread>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/wal_filter.h"
#include "rocksdb/transaction_log.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_batch_base.h"
#include "nlohmann/json.hpp"

using namespace ROCKSDB_NAMESPACE;

// Global variable to track highest key number
int highest_key_num = 0;  // Start from 0, so we'll detect keys starting from 0

void printUsage() {
    std::cout << "Usage: ./rocksdb_init <spikeType> [rocksdbtype|repeatedwrites_mode]\n"
              << "  spikeType: secondarymode | repeatedwrites | scaleout (more types can be added later)\n"
              << "  rocksdbtype: primary | secondary (required only for secondarymode)\n"
              << "  repeatedwrites_mode: standard | update_in_place (required only for repeatedwrites)\n"
              << "For primary mode, you'll be prompted for operations:\n"
              << "  - insert: Enter starting sequence number and count of key-value pairs to insert\n"
              << "  - count: Show total count of entries\n"
              << "  - tail10: Show last 10 entries\n";
}

void handlePrimaryMode(DB* db) {
    while (true) {
        std::cout << "\nEnter operation (insert/count/tail10/exit): ";
        std::string operation;
        std::cin >> operation;

        if (operation == "exit") {
            break;
        } else if (operation == "insert") {
            int start_seq, count;
            std::cout << "Enter starting sequence number: ";
            std::cin >> start_seq;
            std::cout << "Enter number of key-value pairs to insert: ";
            std::cin >> count;

            for (int i = 0; i < count; i++) {
                std::string key = "key" + std::to_string(start_seq + i);
                std::string value = "value" + std::to_string(start_seq + i);
                
                Status s = db->Put(WriteOptions(), key, value);
                if (!s.ok()) {
                    std::cerr << "Error inserting key-value pair: " << s.ToString() << std::endl;
                    return;
                }
            }
            std::cout << "Successfully inserted " << count << " key-value pairs starting from sequence " << start_seq << std::endl;
        } else if (operation == "count") {
            int count = 0;
            Iterator* it = db->NewIterator(ReadOptions());
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                count++;
            }
            delete it;
            std::cout << "Total entries in database: " << count << "\n";
        } else if (operation == "tail10") {
            std::vector<std::pair<std::string, std::string>> entries;
            Iterator* it = db->NewIterator(ReadOptions());
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                entries.push_back({it->key().ToString(), it->value().ToString()});
            }
            delete it;

            std::sort(entries.begin(), entries.end());
            std::cout << "Last 10 entries:\n";
            size_t start_idx = entries.size() > 10 ? entries.size() - 10 : 0;
            for (size_t i = start_idx; i < entries.size(); i++) {
                std::cout << entries[i].first << " : " << entries[i].second << "\n";
            }
        } else {
            std::cout << "Invalid operation. Please try again.\n";
        }
    }
}

// Define WriteBatchHandler at global scope
class MyWriteBatchHandler : public WriteBatch::Handler {
public:
    void Put(const Slice& key, const Slice& value) override {
        std::cout << "PUT: " << key.ToString() << " : " << value.ToString() << std::endl;
    }

    void Delete(const Slice& key) override {
        std::cout << "DELETE: " << key.ToString() << std::endl;
    }

    void LogData(const Slice& blob) override {}
};

void handleSecondaryMode(const std::string& db_path) {
    std::cout << "Secondary mode: Monitoring for new entries...\n";
    
    // Set up secondary instance options
    Options options;
    options.create_if_missing = false;  // Don't create if missing for secondary
    options.max_open_files = -1;        // Keep all files open
    options.write_buffer_size = 500 * 1024; // Set memtable size to 500 KB
    
    // Open as secondary instance
    DB* db;
    Status s = DB::OpenAsSecondary(options, db_path, db_path + "_secondary", &db);
    
    if (!s.ok()) {
        std::cerr << "Error opening secondary instance: " << s.ToString() << std::endl;
        return;
    }

    std::cout << "Secondary instance opened successfully\n";

    while (true) {
        // Try to catch up with the primary
        s = db->TryCatchUpWithPrimary();
        if (!s.ok()) {
            std::cerr << "Error catching up with primary: " << s.ToString() << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        // Read all keys
        std::unique_ptr<Iterator> it(db->NewIterator(ReadOptions()));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string key = it->key().ToString();
            // Extract the number from the key (assuming format "keyN")
            if (key.substr(0, 3) == "key") {
                try {
                    int key_num = std::stoi(key.substr(3));
                    if (key_num > highest_key_num) {
                        std::cout << "New entry detected - Key: " << key 
                                 << ", Value: " << it->value().ToString() << std::endl;
                        highest_key_num = key_num;
                    }
                } catch (const std::exception& e) {
                    // Skip keys that don't match our expected format
                    continue;
                }
            }
        }
        
        if (!it->status().ok()) {
            std::cerr << "Error iterating over keys: " << it->status().ToString() << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    delete db;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printUsage();
        return 1;
    }

    std::string spikeType = argv[1];

    if (spikeType == "secondarymode") {
        if (argc != 3) {
            printUsage();
            return 1;
        }
        std::string rocksdbtype = argv[2];
        if (rocksdbtype != "primary" && rocksdbtype != "secondary") {
            std::cerr << "Invalid rocksdbtype. Must be 'primary' or 'secondary'\n";
            printUsage();
            return 1;
        }
        std::string db_path = "/tmp/rocksdb_example";
        Options options;
        options.create_if_missing = true;
        options.write_buffer_size = 500 * 1024; // Set memtable size to 500 KB
        if (rocksdbtype == "primary") {
            // Open the database as primary
            DB* db;
            Status s = DB::Open(options, db_path, &db);
            if (!s.ok()) {
                std::cerr << "Error opening database: " << s.ToString() << std::endl;
                return 1;
            }
            std::cout << "Database opened at: " << db_path << std::endl;
            handlePrimaryMode(db);
            delete db;
        } else {
            handleSecondaryMode(db_path);
        }
    } else if (spikeType == "repeatedwrites") {
        if (argc != 3) {
            printUsage();
            return 1;
        }
        std::string repeatedwrites_mode = argv[2];
        if (repeatedwrites_mode != "standard" && repeatedwrites_mode != "update_in_place") {
            std::cerr << "Invalid repeatedwrites_mode. Must be 'standard' or 'update_in_place'\n";
            printUsage();
            return 1;
        }
        std::cout << "Running repeatedwrites spikeType in mode: " << repeatedwrites_mode << std::endl;
        // Initialize RocksDB with or without in-place update support
        std::string db_path = "/tmp/rocksdb_example";
        Options options;
        options.create_if_missing = true;
        options.write_buffer_size = 500 * 1024; // Set memtable size to 500 KB
        if (repeatedwrites_mode == "update_in_place") {
            options.inplace_update_support = true;
            options.allow_concurrent_memtable_write = false;
        }
        DB* db;
        Status s = DB::Open(options, db_path, &db);
        if (!s.ok()) {
            std::cerr << "Error opening database: " << s.ToString() << std::endl;
            return 1;
        }
        std::cout << "Database opened at: " << db_path << std::endl;
        // Repeatedly write to the same key with different (shrinking) values
        std::string key = "repeatedwriteskey";
        // Hardcoded random JSON object of about 10 KB
        nlohmann::json base_json = {
            {"id", 1},
            {"name", "user_example"},
            {"timestamp", 0},
            {"data", std::string(9500, 'x')}, // Large random-like string
            {"meta", {
                {"field1", "value1"},
                {"field2", "value2"},
                {"field3", 12345},
                {"field4", 67890}
            }}
        };
        int i = 0;
        while (true) {
            // Mutate a few fields in the JSON
            base_json["id"] = i;
            base_json["timestamp"] = std::time(nullptr);
            base_json["meta"]["field3"] = 12345 + i;
            base_json["meta"]["field4"] = 67890 - i;
            // Serialize to string
            std::string value = base_json.dump();
            Status s = db->Put(WriteOptions(), key, value);
            if (!s.ok()) {
                std::cerr << "Error writing to key: " << s.ToString() << std::endl;
                break;
            }
            if ((i + 1) % 10000 == 0) {
                std::cout << "Updated '" << key << "' " << (i + 1) << " times" << std::endl;
            }
            ++i;
        }
        // delete db; // unreachable, but kept for clarity
    } else if (spikeType == "scaleout") {
        if (argc != 3) {
            printUsage();
            return 1;
        }
        int instanceCount = std::stoi(argv[2]);
        if (instanceCount <= 0) {
            std::cerr << "instanceCount must be > 0\n";
            return 1;
        }
        std::vector<DB*> dbs(instanceCount, nullptr);
        std::vector<std::string> db_paths(instanceCount);
        Options options;
        options.create_if_missing = true;
        options.write_buffer_size = 500 * 1024;
        options.inplace_update_support = true;
        options.allow_concurrent_memtable_write = false;
        for (int i = 0; i < instanceCount; ++i) {
            db_paths[i] = "/tmp/rocksdb_example_" + std::to_string(i+1);
            Status s = DB::Open(options, db_paths[i], &dbs[i]);
            if (!s.ok()) {
                std::cerr << "Error opening database instance " << (i+1) << ": " << s.ToString() << std::endl;
                return 1;
            }
        }
        // Prepare a JSON value
        nlohmann::json base_json = {
            {"id", 1},
            {"name", "user_example"},
            {"timestamp", 0},
            {"data", std::string(9500, 'x')},
            {"meta", {
                {"field1", "value1"},
                {"field2", "value2"},
                {"field3", 12345},
                {"field4", 67890}
            }}
        };
        std::string value = base_json.dump();
        std::vector<int> counters(instanceCount, 0);
        std::vector<std::thread> writers;
        for (int i = 0; i < instanceCount; ++i) {
            writers.emplace_back([i, &dbs, &counters, value]() {
                while (true) {
                    WriteBatch batch;
                    for (int k = 1; k <= 100; ++k) {
                        std::string key = "key" + std::to_string(k);
                        batch.Put(key, value);
                    }
                    Status s = dbs[i]->Write(WriteOptions(), &batch);
                    if (!s.ok()) {
                        std::cerr << "Error writing batch to instance " << (i+1) << ": " << s.ToString() << std::endl;
                    } else {
                        counters[i] += 100;
                        std::cout << "Wrote 100 messages to instance " << (i+1) << ", total: " << counters[i] << std::endl;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            });
        }
        // Hold the process alive
        for (auto& t : writers) {
            t.join();
        }
        // Cleanup (unreachable)
        for (auto db : dbs) {
            delete db;
        }
    } else {
        std::cerr << "Unknown or unimplemented spikeType. Only 'secondarymode', 'repeatedwrites', and 'scaleout' are supported for now.\n";
        printUsage();
        return 1;
    }
    return 0;
} 