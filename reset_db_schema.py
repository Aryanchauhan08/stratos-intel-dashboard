import sys
from pathlib import Path

# Ensure project root is in path so we can import 'database.models'
project_root = str(Path(__file__).resolve().parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from database.models import Base, engine, SocialActivity, ProcessedActivity

def reset_social_activity_table():
    """
    DANGEROUS: Drops and completely recreates the social_activity
    and processed_activity tables from scratch.
    All data inside these tables will be permanently deleted!
    """
    print("WARNING: This will delete ALL data in social_activity and processed_activity tables.")
    confirmation = input("Type 'YES' to continue: ")
    
    if confirmation != "YES":
        print("Aborting database reset.")
        return

    print("Connecting to database and dropping tables...")
    
    # We must also drop ProcessedActivity because it has a Foreign Key constraint
    # pointing to SocialActivity.id
    ProcessedActivity.__table__.drop(engine, checkfirst=True)
    SocialActivity.__table__.drop(engine, checkfirst=True)

    print("Tables dropped. Recreating from updated SQLAlchemy models...")
    
    # Recreate the tables perfectly mirroring the current models.py schema
    Base.metadata.create_all(engine)
    
    print("\nDatabase reset complete! The 'external_id' column is now active.")
    print("You can safely start your background worker and fetcher scripts.")

if __name__ == "__main__":
    reset_social_activity_table()
