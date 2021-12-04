/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"

#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "filescan.h"

//#define DEBUG

namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string &relationName,
                       std::string &outIndexName,
                       BufMgr *bufMgrIn,
                       const int attrByteOffset,
                       const Datatype attrType) {
    // Creating the index file name, taken from the project specification
    std ::ostringstream indexStr;
    indexStr << relationName << '.' << attrByteOffset;
    outIndexName = indexStr.str();

    Page *headerPage;
    file = new BlobFile(outIndexName, false);
    headerPageNum = file->getFirstPageNo();

    bufMgrIn->readPage(file, headerPageNum, headerPage);
    IndexMetaInfo *meta = (IndexMetaInfo *)headerPage;
    if (relationName != meta->relationName) throw BadIndexInfoException("Index doesn't exist.");
    if (attributeType != meta->attrType) throw BadIndexInfoException("Index doesn't exist.");
    if (attrByteOffset != meta->attrByteOffset) throw BadIndexInfoException("Index  doesn't exist.");

    // bufMgrIn->readPage()
    // if (relationName != meta)
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void *lowValParm,
                           const Operator lowOpParm,
                           const void *highValParm,
                           const Operator highOpParm) {
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId &outRid) {
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() {
}

}  // namespace badgerdb
