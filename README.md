# bptdb

bptdb(b-plus-tree database)是c++编写的基于b+tree的key-value存储引擎。

## 构建安装

获取源代码

```
git clone https://github.com/btGuo/bptdb.git
```

对于posix

```
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make 
sudo make install
```

## 使用文档

#### 打开数据库

```
bptdb::DB db;
auto stat = db.open("my.db", bptdb::DB_CREATE);
assert(stat.ok());
```

使用了DB_CREATE选项如果数据库不存在则会创建数据库，如果想要直接报错，去掉DB_CREATE即可，如下

```
auto stat = db.open("my.db");
```

或者可以直接创建数据库，指定的数据库文件会被清空

```
auto stat = db.create("my.db");
```

以上stat的类型是bptdb::Status，出错时可打印错误信息

```
if(!stat.ok()) {
	std::cout << stat.getErrmsg() << std::endl;
	return 0;
}
```

db析构时，数据库会安全关闭。

#### 使用bucket

bucket相当于mysql中的表，同一个bucket内key是唯一的，不同的bucket可以存储不同的key。

创建bucket

```
auto [stat, bucket] = db.createBucket("mybucket");
assert(stat.ok());
```

以上在db中创建了一个名为mybucket的bucket，返回值是std::tuple<bptdb::Status, bptdb::Bucket>类型，可以利用c++17特性结构化绑定返回值。

获取bucket

```
auto [stat, bucket] = db.getBucket("mybucket");
assert(stat.ok());
```

bucket是移动语义，按值传递即可。

#### 数据读写

数据读写都得通过bucket

添加数据

```
auto stat = bucket.put(key, val);
assert(stat.ok());
```

获取数据

```
auto [stat, val] = bucket.get(key);
assert(stat.ok());
```

删除数据

```
auto stat = bucket.del(key);
assert(stat.ok());
```

更新数据

```
auto stat = bucket.update(key, newval);
assert(stat.ok());
```

以上bucket的四个操作put, get, del, update均是线程安全的。

#### 迭代器

**注意: 迭代器是只读的，并且非线程安全。

检查迭代器是否遍历完。

```
bool done();
```

移动迭代器到下一个位置。

```
void next();
```

获取key还有value，注意返回值类型都是string_view，并不拥有string的所有权。

```
std::string_view key();
std::string_view val();
```

多线程环境下为了安全遍历迭代器，应该锁住整个bucket。

获取bucket互斥量，返回值是std::shared_mutex&类型，读写锁

```
bucket.mutex()
```

利用RAII安全上锁，互斥锁或者共享锁。

```
std::shared_lock lg(bucket.mutex());
std::lock_guard lg(bucket.mutex());
```

获取bucket起始迭代器

```
auto it = bucket.begin();
```

获取确定位置迭代器，以下获取key处迭代器

```
auto it = bucket.at(key);
```

安全遍历容器

```
{
	std::shared_lock lg(bucket.mutex());
	for(auto it = bucket.begin(); !it->done(); it->next()) {
		std::cout << "key " << it->key() << " val " << it->val() << std::endl;
	}
}
```

## 使用的c++17特性

```
std::shared_mutex
std::string_view
structured binding
```

## 性能测试

测试数据集

```
key长度:    10 - 40 byte
value长度: 50 - 100 byte
数据量:       100w
线程数:        1
数据大小:   100M
```

cpu硬件信息

```
CPU: 架构：             x86_64
CPU 运行模式：     32-bit, 64-bit
字节序：        		 	Little Endian
CPU:             			   8
在线 CPU 列表：    0-7
每个核的线程数： 2
每个座的核数：   	4
座：           				 1
NUMA 节点：      	 1
厂商 ID：        		    GenuineIntel
CPU 系列：       	    6
型号：           			  94
型号名称：              Intel(R) Core(TM) i7-6700HQ CPU @ 2.60GHz
步进：           			  3
CPU MHz：        	 2910.739
CPU 最大 MHz：   3500.0000
CPU 最小 MHz：   800.0000
BogoMIPS：       	5199.98
虚拟化：         	   	   VT-x
L1d 缓存：       		  32K
L1i 缓存：              	32K
L2 缓存：        			256K
L3 缓存：        		    6144K
NUMA 节点0 CPU: 0-7
```

硬盘信息

```
Model Family:     HGST Travelstar 7K1000                                                                                  Device Model:     HGST HTS721010A9E630                                                                                Serial Number:    JR10004M2GV4JF                                                                                            LU WWN Device Id: 5 000cca 8a8e2ee5a                                                                                    Firmware Version: JB0OA3J0                                                                                                          User Capacity:    1,000,204,886,016 bytes [1.00 TB]                                                              Sector Sizes:     512 bytes logical, 4096 bytes physical                                                        Rotation Rate:    7200 rpm                                                                                                                Form Factor:      2.5 inches                                                                                                               Device is:        In smartctl database [for details use: -P show]                                            ATA Version is:   ATA8-ACS T13/1699-D revision 6                                                                 SATA Version is:  SATA 3.0, 6.0 Gb/s (current: 6.0 Gb/s)                                                        Local Time is:    Sun Feb 16 22:29:49 2020 CST                                                                       SMART support is: Available - device has SMART capability.                                            SMART support is: Enabled      
```

随机写入(微妙每操作)

```
12.1687 micro/op
```

随机读取(微妙每操作)

```
6.64167 micro/op
```

顺序读取(微妙每操作)

```
0.148999 micro/op
```

