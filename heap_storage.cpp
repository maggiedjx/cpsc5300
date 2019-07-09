#include "heap_storage.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include <cstring>
#include "SQLParser.h"
#include "sqlhelper.h"

#define DB_BLOCK_SZ 256
DbEnv* _DB_ENV;
typedef u_int16_t u16;

// SlottedPage
SlottedPage::SlottedPage(Dbt &block, 
                        BlockID block_id, 
                        bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

Dbt* SlottedPage::get(RecordID record_id) {
    get_header(this->num_records, this->end_free, record_id);

    return 0;
    // size, loc = self._get_header(id)
    // if loc == 0:
    //     return None  # this is just a tombstone, record has been deleted
    // return self.block[loc:loc + size]
}

void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError) {
    // """ Replace the record with the given data. Raises ValueError if it won't fit. """
    // size, loc = self._get_header(id)
    // new_size = len(data)
    // if new_size > size:
    //     extra = new_size - size
    //     if not self._has_room(extra):
    //         raise ValueError('Not enough room in block')
    //     self._slide(loc + new_size, loc + size)
    //     self.block[loc - extra:loc + new_size] = data
    // else:
    //     self.block[loc:loc + new_size] = data
    //     self._slide(loc + new_size, loc + size)
    // size, loc = self._get_header(id)
    // self._put_header(id, new_size, loc)
}

void SlottedPage::del(RecordID record_id) {
    get_header(this->num_records, this->end_free, record_id);
    put_header(record_id, 0, 0);
    slide(this->end_free, this->end_free + this->num_records);
}

RecordIDs* SlottedPage::ids(void) {
    RecordIDs r;
    return &r;

    // return (i for i in range(1, self.num_records + 1) if self._get_header(i)[1] != 0)
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) throw(DbBlockNoRoomError) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);

    // return self._get_n(4 * id), self._get_n(4 * id + 2)
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

bool SlottedPage::has_room(u_int16_t size) {
    u_int16_t available = this->end_free - (this->num_records + 1) * 4;
    return size <= available;

    // available = self.end_free - (self.num_records + 1) * 4
    // return size <= available
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    // shift = end - start
    // if shift == 0:
    //     return

    // # slide data
    // self.block[self.end_free + 1 + shift: end] = self.block[self.end_free + 1: start]

    // # fixup headers
    // for id in self.ids():
    //     size, loc = self._get_header(id)
    //     if loc <= start:
    //         loc += shift
    //         self._put_header(id, size, loc)
    // self.end_free += shift
    // self._put_header()
}

void HeapFile::create(void) {
    // self._db_open(bdb.DB_CREATE | bdb.DB_EXCL)
    // block = self.get_new()  # first block of the file
    // self.put(block)
}

void HeapFile::drop(void) {
    // self.close()
    // os.remove(self.dbfilename)
}

void HeapFile::open(void) {
    db_open();
    // self._db_open()
    // self.block_size = self.stat['re_len']  # what's in the file overrides __init__ parameter
}

void HeapFile::close(void) {
    // self.db.close()
    // self.closed = True
    this->db.close(0);
    this->closed = true;
}

SlottedPage* HeapFile::get(BlockID block_id) {
    Dbt d;
    SlottedPage s(d, block_id);
    return &s;

    // return new SlottedPage(
    //     block=this->db.get(block_id), block_id)
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
    char block[DB_BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

void HeapFile::put(DbBlock* block) {
    // self.db.put(block.id, bytes(block.block))
}

BlockIDs* HeapFile::block_ids() {
    BlockIDs b;
    return &b;

    // return (i for i in range(1, self.last + 1))

}

void HeapFile::db_open(uint flags) {

}

// HeapTable
HeapTable::HeapTable(Identifier table_name, 
                    ColumnNames column_names, 
                    ColumnAttributes column_attributes)
                    : DbRelation(table_name, column_names, column_attributes), file(table_name) {

}

void HeapTable::create() {
    this->file.create();
}

void HeapTable::create_if_not_exists() {
    this->file.open();
    // try:
    //     self.open()
    // except bdb.DBNoSuchFileError:
    //     self.create()
}

void HeapTable::drop() {
    // this->file.delete();
}

void HeapTable::open() {
    this->file.open();
}

void HeapTable::close() {
    this->file.close();
}

Handle HeapTable::insert(const ValueDict* row) {
    Handle h;
    return h;

    // self.open()
    // return self._append(self._validate(row))
}

void HeapTable::update(const Handle handle, const ValueDict* new_values) {
    // exception?
}

void HeapTable::del(const Handle handle) {
    // exception?
}

Handles* HeapTable::select() {
    Handles h;
    return &h;

    // for block_id in self.file.block_ids():
    // for record_id in self.file.get(block_id).ids():
    //     yield (block_id, record_id)
}

Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

ValueDict* HeapTable::project(Handle handle) {
    ColumnNames* v;
    return project(handle, v);

    // project(self, handle, column_names=None):
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names) {
    ValueDict v;
    return &v;

    // block_id, record_id = handle
    // block = self.file.get(block_id)
    // data = block.get(record_id)
    // row = self._unmarshal(data)
    // if column_names is None:
    //     return row
    // else:
    //     return {k: row[k] for k in column_names}
}

ValueDict* HeapTable::validate(const ValueDict* row) {
    ValueDict v;
    return &v;

    // full_row = {}
    // for column_name in self.columns:
    //     column = self.columns[column_name]
    //     if column_name not in row:
    //         raise ValueError("don't know how to handle NULLs, defaults, etc. yet")
    //     else:
    //         value = row[column_name]
    //     full_row[column_name] = value
    // return full_row
}

Handle HeapTable::append(const ValueDict* row) {
    Handle h;
    return h;

    // data = self._marshal(row)
    // block = self.file.get(self.file.last)
    // try:
    //     record_id = block.add(data)
    // except ValueError:
    //     # need a new block
    //     block = self.file.get_new()
    //     record_id = block.add(data)
    // self.file.put(block)
    // return self.file.last, record_id
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict* HeapTable::unmarshal(Dbt* data) {
    ValueDict v;
    return &v;

    // row = {}
    // offset = 0
    // for column_name in self.column_names:
    //     column = self.columns[column_name]
    //     if column['data_type'] == 'INT':
    //         row[column_name] = int.from_bytes(data[offset:offset + 4], byteorder='big', signed=True)
    //         offset += 4
    //     elif column['data_type'] == 'TEXT':
    //         size = int.from_bytes(data[offset:offset + 2], byteorder='big')
    //         offset += 2
    //         row[column_name] = data[offset:offset + size].decode('utf-8')
    //         offset += size
    //     else:
    //         raise ValueError('Cannot unmarahal ' + column['data_type'])
    // return row
}

// test function -- returns true if all tests pass
bool test_heap_storage() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
    	return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
		return false;
    table.drop();

    return true;
}