//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"
#include "storage/page/hash_table_directory_page.h"

namespace bustub {

// NOLINTNEXTLINE

// NOLINTNEXTLINE
TEST(HashTableTest, DISABLED_SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  HashTableDirectoryPage *htdp = ht.FetchDirectoryPage();
  htdp->PrintDirectory();

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();
  htdp->PrintDirectory();

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();
  htdp->PrintDirectory();

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  ht.VerifyIntegrity();
  htdp->PrintDirectory();

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 20, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  ht.VerifyIntegrity();
  htdp->PrintDirectory();
  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }

  ht.VerifyIntegrity();
  htdp->PrintDirectory();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, ManyInsertTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  HashTableDirectoryPage *htdp = ht.FetchDirectoryPage();

  for (int i = 0; i < 5000; i++) {
    EXPECT_TRUE(ht.Insert(nullptr, i, i));
  }
  for (int i = 0; i < 5000; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size());
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();
  htdp->PrintDirectory();

  for (int i = 0; i < 5000; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();
  htdp->PrintDirectory();

  for (int i = 5000; i < 10000; i++) {
    EXPECT_TRUE(ht.Insert(nullptr, i, i));
  }

  for (int i = 5000; i < 10000; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size());
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();
  htdp->PrintDirectory();

  for (int i = 5000; i < 10000; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();
  ht.PrintDirectoryAndBuckets();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, DISABLED_ConcurrentInsertTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  std::thread t1([&ht]() {
    // insert many values
    for (int i = 0; i < 10000; i++) {
      EXPECT_TRUE(ht.Insert(nullptr, i, i));
    }
  });

  std::thread t2([&ht]() {
    // insert many values
    for (int i = 10000; i < 20000; i++) {
      EXPECT_TRUE(ht.Insert(nullptr, i, i));
    }
  });

  t1.join();
  t2.join();

  for (int i = 0; i < 4000; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size());
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();
  ht.PrintDirectoryAndBuckets();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, ConcurrentInsertRemoveTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // HashTableDirectoryPage *htdp = ht.FetchDirectoryPage();

  std::thread t1([&ht]() {
    // insert many values
    for (int i = 0; i < 10000; i++) {
      EXPECT_TRUE(ht.Insert(nullptr, i, i));
    }

    // for (int i = 0; i < 5000; i++) {
    //   EXPECT_TRUE(ht.Remove(nullptr, i, i));
    // }
  });

  std::thread t2([&ht]() {
    // insert many values
    for (int i = 10000; i < 20000; i++) {
      EXPECT_TRUE(ht.Insert(nullptr, i, i));
    }

    // for (int i = 5000; i < 10000; i++) {
    //   EXPECT_TRUE(ht.Remove(nullptr, i, i));
    // }
  });

  std::thread t3([&ht]() {
    // insert many values
    for (int i = 20000; i < 30000; i++) {
      EXPECT_TRUE(ht.Insert(nullptr, i, i));
    }

    // for (int i = 10000; i < 15000; i++) {
    //   EXPECT_TRUE(ht.Remove(nullptr, i, i));
    // }
  });

  t1.join();
  t2.join();
  t3.join();

  for (int i = 0; i < 30000; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
  }
  // for (int i = 0; i < 5000; i++) {
  //   std::vector<int> res;
  //   ht.GetValue(nullptr, i, &res);
  //   EXPECT_EQ(1, res.size());
  //   EXPECT_EQ(i, res[0]);
  // }

  // ht.VerifyIntegrity();
  // htdp->PrintDirectory();

  // for (int i = 0; i < 5000; i++) {
  //   EXPECT_TRUE(ht.Remove(nullptr, i, i));
  // }

  // ht.VerifyIntegrity();
  // htdp->PrintDirectory();

  // for (int i = 0; i < 15000; ++i) {
  //   std::vector<int> res;
  //   ht.GetValue(nullptr, i, &res);
  //   EXPECT_EQ(0, res.size());
  // }

  ht.VerifyIntegrity();
  ht.PrintDirectoryAndBuckets();
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, ConcurrentInsertConcurrentRemoveTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // HashTableDirectoryPage *htdp = ht.FetchDirectoryPage();

  std::thread t1([&ht]() {
    // insert many values
    for (int i = 0; i < 10000; i++) {
      EXPECT_TRUE(ht.Insert(nullptr, i, i));
    }

    for (int i = 0; i < 10000; i++) {
      EXPECT_TRUE(ht.Remove(nullptr, i, i));
    }
  });

  std::thread t2([&ht]() {
    // insert many values
    for (int i = 10000; i < 20000; i++) {
      EXPECT_TRUE(ht.Insert(nullptr, i, i));
    }

    for (int i = 10000; i < 20000; i++) {
      EXPECT_TRUE(ht.Remove(nullptr, i, i));
    }
  });

  std::thread t3([&ht]() {
    // insert many values
    for (int i = 20000; i < 30000; i++) {
      EXPECT_TRUE(ht.Insert(nullptr, i, i));
    }

    for (int i = 20000; i < 30000; i++) {
      EXPECT_TRUE(ht.Remove(nullptr, i, i));
    }
  });

  t1.join();
  t2.join();
  t3.join();


  ht.VerifyIntegrity();
  ht.PrintDirectoryAndBuckets();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}


TEST(HashTableTest, SimpleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  HashTableDirectoryPage *htdp = ht.FetchDirectoryPage();

  for (int i = 0; i < 10000; i++) {
    EXPECT_TRUE(ht.Insert(nullptr, i, i));
  }

  ht.VerifyIntegrity();
  htdp->PrintDirectory();

  for (int i = 0; i < 10000; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
  }

  ht.VerifyIntegrity();
  ht.PrintDirectoryAndBuckets();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}
}  // namespace bustub
