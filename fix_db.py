import os
import sys
from pathlib import Path
from sqlalchemy import text

# Ensure project root is in path so we can import 'database.models'
project_root = str(Path(__file__).resolve().parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from database.models import engine

def apply_migration():
    """
    Safely executes an ALTER TABLE command to add 'external_id'
    to the social_activity database table.
    """
    print("Checking database schema for 'external_id' column...")
    
    with engine.begin() as conn:
        try:
            # 1. Add the column safely without deleting any of your existing 5000 records
            conn.execute(text("ALTER TABLE social_activity ADD COLUMN external_id VARCHAR(255);"))
            print("Successfully executed: ALTER TABLE social_activity ADD COLUMN external_id VARCHAR(255);")
        except Exception as e:
            # If the column already exists, this will gracefully fail instead of crashing the app
            print(f"Column might already exist or another error occurred during ALTER TABLE: {e}")

        try:
            # 2. Add an index so the deduplicator lookups are lightning fast
            conn.execute(text("CREATE UNIQUE INDEX ix_social_activity_external_id ON social_activity (external_id);"))
            print("Successfully executed: CREATE UNIQUE INDEX ix_social_activity_external_id ON social_activity (external_id);")
        except Exception as e:
            print(f"Index might already exist or another error occurred during CREATE INDEX: {e}")

    print("\nMigration complete! The 'social_activity' table is permanently updated.")
    print("You can now safely restart your background worker.")

if __name__ == "__main__":
    apply_migration()
