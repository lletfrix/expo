//  Copyright (c) 2020 650 Industries, Inc. All rights reserved.

import ExpoModulesTestCore
import SQLite3

@testable import EXUpdates

let EXUpdatesDatabaseV4Schema = """
  CREATE TABLE "updates" (
    "id"  BLOB UNIQUE,
    "scope_key"  TEXT NOT NULL,
    "commit_time"  INTEGER NOT NULL,
    "runtime_version"  TEXT NOT NULL,
    "launch_asset_id" INTEGER,
    "metadata"  TEXT,
    "status"  INTEGER NOT NULL,
    "keep"  INTEGER NOT NULL,
    PRIMARY KEY("id"),
    FOREIGN KEY("launch_asset_id") REFERENCES "assets"("id") ON DELETE CASCADE
  );
  CREATE TABLE "assets" (
    "id"  INTEGER PRIMARY KEY AUTOINCREMENT,
    "url"  TEXT,
    "key"  TEXT NOT NULL UNIQUE,
    "headers"  TEXT,
    "type"  TEXT NOT NULL,
    "metadata"  TEXT,
    "download_time"  INTEGER NOT NULL,
    "relative_path"  TEXT NOT NULL,
    "hash"  BLOB NOT NULL,
    "hash_type"  INTEGER NOT NULL,
    "marked_for_deletion"  INTEGER NOT NULL
  );
  CREATE TABLE "updates_assets" (
    "update_id"  BLOB NOT NULL,
    "asset_id" INTEGER NOT NULL,
    FOREIGN KEY("update_id") REFERENCES "updates"("id") ON DELETE CASCADE,
    FOREIGN KEY("asset_id") REFERENCES "assets"("id") ON DELETE CASCADE
  );
  CREATE TABLE "json_data" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "key" TEXT NOT NULL,
    "value" TEXT NOT NULL,
    "last_updated" INTEGER NOT NULL,
    "scope_key" TEXT NOT NULL
  );
  CREATE UNIQUE INDEX "index_updates_scope_key_commit_time" ON "updates" ("scope_key", "commit_time");
  CREATE INDEX "index_updates_launch_asset_id" ON "updates" ("launch_asset_id");
  CREATE INDEX "index_json_data_scope_key" ON "json_data" ("scope_key");
"""

let EXUpdatesDatabaseV5Schema = """
  CREATE TABLE "updates" (
    "id"  BLOB UNIQUE,
    "scope_key"  TEXT NOT NULL,
    "commit_time"  INTEGER NOT NULL,
    "runtime_version"  TEXT NOT NULL,
    "launch_asset_id" INTEGER,
    "metadata"  TEXT,
    "status"  INTEGER NOT NULL,
    "keep"  INTEGER NOT NULL,
    PRIMARY KEY("id"),
    FOREIGN KEY("launch_asset_id") REFERENCES "assets"("id") ON DELETE CASCADE
  );
  CREATE TABLE "assets" (
    "id"  INTEGER PRIMARY KEY AUTOINCREMENT,
    "url"  TEXT,
    "key"  TEXT UNIQUE,
    "headers"  TEXT,
    "type"  TEXT NOT NULL,
    "metadata"  TEXT,
    "download_time"  INTEGER NOT NULL,
    "relative_path"  TEXT NOT NULL,
    "hash"  BLOB NOT NULL,
    "hash_type"  INTEGER NOT NULL,
    "marked_for_deletion"  INTEGER NOT NULL
  );
  CREATE TABLE "updates_assets" (
    "update_id"  BLOB NOT NULL,
    "asset_id" INTEGER NOT NULL,
    FOREIGN KEY("update_id") REFERENCES "updates"("id") ON DELETE CASCADE,
    FOREIGN KEY("asset_id") REFERENCES "assets"("id") ON DELETE CASCADE
  );
  CREATE TABLE "json_data" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "key" TEXT NOT NULL,
    "value" TEXT NOT NULL,
    "last_updated" INTEGER NOT NULL,
    "scope_key" TEXT NOT NULL
  );
  CREATE UNIQUE INDEX "index_updates_scope_key_commit_time" ON "updates" ("scope_key", "commit_time");
  CREATE INDEX "index_updates_launch_asset_id" ON "updates" ("launch_asset_id");
  CREATE INDEX "index_json_data_scope_key" ON "json_data" ("scope_key");
"""

let EXUpdatesDatabaseV6Schema = """
  CREATE TABLE "updates" (
    "id"  BLOB UNIQUE,
    "scope_key"  TEXT NOT NULL,
    "commit_time"  INTEGER NOT NULL,
    "runtime_version"  TEXT NOT NULL,
    "launch_asset_id" INTEGER,
    "manifest"  TEXT,
    "status"  INTEGER NOT NULL,
    "keep"  INTEGER NOT NULL,
    "last_accessed"  INTEGER NOT NULL,
    PRIMARY KEY("id"),
    FOREIGN KEY("launch_asset_id") REFERENCES "assets"("id") ON DELETE CASCADE
  );
  CREATE TABLE "assets" (
    "id"  INTEGER PRIMARY KEY AUTOINCREMENT,
    "url"  TEXT,
    "key"  TEXT UNIQUE,
    "headers"  TEXT,
    "type"  TEXT NOT NULL,
    "metadata"  TEXT,
    "download_time"  INTEGER NOT NULL,
    "relative_path"  TEXT NOT NULL,
    "hash"  BLOB NOT NULL,
    "hash_type"  INTEGER NOT NULL,
    "marked_for_deletion"  INTEGER NOT NULL
  );
  CREATE TABLE "updates_assets" (
    "update_id"  BLOB NOT NULL,
    "asset_id" INTEGER NOT NULL,
    FOREIGN KEY("update_id") REFERENCES "updates"("id") ON DELETE CASCADE,
    FOREIGN KEY("asset_id") REFERENCES "assets"("id") ON DELETE CASCADE
  );
  CREATE TABLE "json_data" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "key" TEXT NOT NULL,
    "value" TEXT NOT NULL,
    "last_updated" INTEGER NOT NULL,
    "scope_key" TEXT NOT NULL
  );
  CREATE UNIQUE INDEX "index_updates_scope_key_commit_time" ON "updates" ("scope_key", "commit_time");
  CREATE INDEX "index_updates_launch_asset_id" ON "updates" ("launch_asset_id");
  CREATE INDEX "index_json_data_scope_key" ON "json_data" ("scope_key");
"""

let EXUpdatesDatabaseV7Schema = """
  CREATE TABLE "updates" (
    "id"  BLOB UNIQUE,
    "scope_key"  TEXT NOT NULL,
    "commit_time"  INTEGER NOT NULL,
    "runtime_version"  TEXT NOT NULL,
    "launch_asset_id" INTEGER,
    "manifest"  TEXT,
    "status"  INTEGER NOT NULL,
    "keep"  INTEGER NOT NULL,
    "last_accessed"  INTEGER NOT NULL,
    "successful_launch_count"  INTEGER NOT NULL DEFAULT 0,
    "failed_launch_count"  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY("id"),
    FOREIGN KEY("launch_asset_id") REFERENCES "assets"("id") ON DELETE CASCADE
  );
  CREATE TABLE "assets" (
    "id"  INTEGER PRIMARY KEY AUTOINCREMENT,
    "url"  TEXT,
    "key"  TEXT UNIQUE,
    "headers"  TEXT,
    "type"  TEXT NOT NULL,
    "metadata"  TEXT,
    "download_time"  INTEGER NOT NULL,
    "relative_path"  TEXT NOT NULL,
    "hash"  BLOB NOT NULL,
    "hash_type"  INTEGER NOT NULL,
    "marked_for_deletion"  INTEGER NOT NULL
  );
  CREATE TABLE "updates_assets" (
    "update_id"  BLOB NOT NULL,
    "asset_id" INTEGER NOT NULL,
    FOREIGN KEY("update_id") REFERENCES "updates"("id") ON DELETE CASCADE,
    FOREIGN KEY("asset_id") REFERENCES "assets"("id") ON DELETE CASCADE
  );
  CREATE TABLE "json_data" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "key" TEXT NOT NULL,
    "value" TEXT NOT NULL,
    "last_updated" INTEGER NOT NULL,
    "scope_key" TEXT NOT NULL
  );
  CREATE UNIQUE INDEX "index_updates_scope_key_commit_time" ON "updates" ("scope_key", "commit_time");
  CREATE INDEX "index_updates_launch_asset_id" ON "updates" ("launch_asset_id");
  CREATE INDEX "index_json_data_scope_key" ON "json_data" ("scope_key");
"""

let EXUpdatesDatabaseV8Schema = """
  CREATE TABLE "updates" (
    "id"  BLOB UNIQUE,
    "scope_key"  TEXT NOT NULL,
    "commit_time"  INTEGER NOT NULL,
    "runtime_version"  TEXT NOT NULL,
    "launch_asset_id" INTEGER,
    "manifest"  TEXT,
    "status"  INTEGER NOT NULL,
    "keep"  INTEGER NOT NULL,
    "last_accessed"  INTEGER NOT NULL,
    "successful_launch_count"  INTEGER NOT NULL DEFAULT 0,
    "failed_launch_count"  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY("id"),
    FOREIGN KEY("launch_asset_id") REFERENCES "assets"("id") ON DELETE CASCADE
  );
  CREATE TABLE "assets" (
    "id"  INTEGER PRIMARY KEY AUTOINCREMENT,
    "url"  TEXT,
    "key"  TEXT UNIQUE,
    "headers"  TEXT,
    "extra_request_headers"  TEXT,
    "type"  TEXT NOT NULL,
    "metadata"  TEXT,
    "download_time"  INTEGER NOT NULL,
    "relative_path"  TEXT NOT NULL,
    "hash"  BLOB NOT NULL,
    "hash_type"  INTEGER NOT NULL,
    "marked_for_deletion"  INTEGER NOT NULL
  );
  CREATE TABLE "updates_assets" (
    "update_id"  BLOB NOT NULL,
    "asset_id" INTEGER NOT NULL,
    FOREIGN KEY("update_id") REFERENCES "updates"("id") ON DELETE CASCADE,
    FOREIGN KEY("asset_id") REFERENCES "assets"("id") ON DELETE CASCADE
  );
  CREATE TABLE "json_data" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "key" TEXT NOT NULL,
    "value" TEXT NOT NULL,
    "last_updated" INTEGER NOT NULL,
    "scope_key" TEXT NOT NULL
  );
  CREATE UNIQUE INDEX "index_updates_scope_key_commit_time" ON "updates" ("scope_key", "commit_time");
  CREATE INDEX "index_updates_launch_asset_id" ON "updates" ("launch_asset_id");
  CREATE INDEX "index_json_data_scope_key" ON "json_data" ("scope_key");
"""

class EXUpdatesDatabaseInitializationSpec : ExpoSpec {
  override func spec() {
    var testDatabaseDir: URL!
    
    beforeEach {
      let applicationSupportDir = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).last
      testDatabaseDir = applicationSupportDir!.appendingPathComponent("EXUpdatesDatabaseTests")
      
      if FileManager.default.fileExists(atPath: testDatabaseDir.path) {
        try! FileManager.default.createDirectory(atPath: testDatabaseDir.path, withIntermediateDirectories: true)
      }
    }
    
    afterEach {
      try! FileManager.default.removeItem(atPath: testDatabaseDir.path)
    }
    
    describe("database persistence") {
      it("persists") {
        let db = try! EXUpdatesDatabaseInitialization.initializeDatabaseWithLatestSchema(inDirectory: testDatabaseDir)
        
        // insert some test data
        let insertSql = """
          INSERT INTO "assets" ("url","key","headers","type","metadata","download_time","relative_path","hash","hash_type","marked_for_deletion")
            VALUES (NULL,'bundle-1614137401950',NULL,'js',NULL,1614137406588,'bundle-1614137401950','6ff4ee75b48a21c7a9ed98015ff6bfd0a47b94cd087c5e2258262e65af239952',0,0);
        """
        _ = try! EXUpdatesDatabaseUtils.execute(sql: insertSql, withArgs: nil, onDatabase: db)
        
        // mimic the app closing and reopening
        sqlite3_close(db)
        let newDb = try! EXUpdatesDatabaseInitialization.initializeDatabaseWithLatestSchema(inDirectory: testDatabaseDir)
        
        // ensure the data is still there
        let selectSql = "SELECT * FROM `assets` WHERE `url` IS NULL AND `key` = 'bundle-1614137401950' AND `headers` IS NULL AND `type` = 'js' AND `metadata` IS NULL AND `download_time` = 1614137406588 AND `relative_path` = 'bundle-1614137401950' AND `hash` = '6ff4ee75b48a21c7a9ed98015ff6bfd0a47b94cd087c5e2258262e65af239952' AND `hash_type` = 0 AND `marked_for_deletion` = 0"
        let rows = try! EXUpdatesDatabaseUtils.execute(sql: selectSql, withArgs: nil, onDatabase: newDb)
        expect(rows.count) == 1
        expect(rows[0]["id"] as? Int) == 1
      }
    }
  }
}
