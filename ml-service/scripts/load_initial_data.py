"""
# MongoDB Atlas Data Loader for Sri Lanka Tour Guide

This script loads initial data from CSV and JSON files into MongoDB Atlas collections.

## What it does:
- Reads reviews from CSV (final.csv)
- Reads location details from JSON (sri_lanka_locations_batch2.json)
- Connects to MongoDB Atlas cloud database
- Creates validated collections with schema enforcement
- Loads data with progress tracking
- Creates optimized indexes for fast queries
- Generates comprehensive statistics

## Usage:
    python scripts/load_initial_data.py

## Requirements:
- MongoDB Atlas account and connection string in .env
- CSV and JSON data files in data/raw/
- Python packages: pymongo, tqdm, python-dotenv
"""

import csv
import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path

# Third-party imports
try:
    from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT, GEOSPHERE
    from pymongo.errors import (
        ConnectionFailure,
        DuplicateKeyError,
        BulkWriteError,
        OperationFailure
    )
    from tqdm import tqdm
    from dotenv import load_dotenv
except ImportError as e:
    print(f"❌ Error: Missing required package - {e}")
    print("\n📦 Please install required packages:")
    print("   pip install pymongo tqdm python-dotenv")
    sys.exit(1)


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration settings for MongoDB Atlas data loader"""

    # Load environment variables from .env file
    load_dotenv()

    # File paths (relative to project root)
    BASE_DIR = Path(__file__).parent.parent.parent  # Go up to project root
    CSV_FILE = BASE_DIR / "data" / "raw" / "final.csv"
    JSON_FILE = BASE_DIR / "data" / "raw" / "sri_lanka_locations_batch2.json"

    # MongoDB Atlas connection
    # Get from environment variable or use default Atlas connection
    MONGO_URI = os.getenv(
        "MONGO_URI",
        "mongodb+srv://pgmsadeep:1234@cluster0.phudmlq.mongodb.net/?retryWrites=true&w=majority"
    )
    DATABASE_NAME = os.getenv("DB_NAME", "aiTourGuide")

    # Collection names
    REVIEWS_COLLECTION = "reviews"
    LOCATIONS_COLLECTION = "locations"

    # Batch size for bulk operations (optimized for Atlas)
    BATCH_SIZE = 100

    # Connection timeout (seconds)
    CONNECTION_TIMEOUT = 10000  # 10 seconds for Atlas


# ============================================================================
# DATA VALIDATION
# ============================================================================

class DataValidator:
    """
    Validates data before insertion into MongoDB

    Ensures data quality and consistency by checking:
    - Required fields presence
    - Data types and formats
    - Value constraints
    - GeoJSON coordinate validity
    """

    @staticmethod
    def validate_review(row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and transform a review row from CSV

        Args:
            row: Dictionary containing review data from CSV
                 Expected keys: Destination, District, Location_Type, Review

        Returns:
            Validated and transformed review document ready for MongoDB

        Raises:
            ValueError: If required fields are missing or invalid

        Example:
            >>> row = {'Destination': 'Sigiriya', 'District': 'Matale',
                      'Location_Type': 'Historical', 'Review': 'Amazing place!'}
            >>> validated = DataValidator.validate_review(row)
        """
        required_fields = ['Destination', 'District', 'Location_Type', 'Review']

        # Check for required fields
        for field in required_fields:
            if field not in row or not row[field]:
                raise ValueError(f"Missing required field: {field}")

        # Transform and validate
        review_doc = {
            'destination': row['Destination'].strip(),
            'district': row['District'].strip(),
            'location_type': row['Location_Type'].strip(),
            'review_text': row['Review'].strip(),
            'created_at': datetime.utcnow(),
            'metadata': {
                'source': 'csv_import',
                'import_date': datetime.utcnow()
            }
        }

        # Validate review text length (minimum 10 characters)
        if len(review_doc['review_text']) < 10:
            raise ValueError("Review text too short (minimum 10 characters)")

        return review_doc

    @staticmethod
    def validate_location(location: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and transform a location from JSON

        Args:
            location: Dictionary containing location data
                     Must include: locationId, name, coordinates (GeoJSON)

        Returns:
            Validated location document

        Raises:
            ValueError: If required fields are missing or invalid

        GeoJSON Format:
            coordinates: {
                "type": "Point",
                "coordinates": [longitude, latitude]  # Note: lon, lat order
            }
        """
        required_fields = ['locationId', 'name', 'coordinates']

        # Check for required fields
        for field in required_fields:
            if field not in location:
                raise ValueError(f"Missing required field: {field}")

        # Validate GeoJSON coordinates format
        coords = location.get('coordinates', {})
        if coords.get('type') != 'Point':
            raise ValueError("Invalid coordinate type (must be 'Point')")

        coord_array = coords.get('coordinates', [])
        if len(coord_array) != 2:
            raise ValueError("Invalid coordinates format (must be [longitude, latitude])")

        # Validate longitude and latitude ranges
        longitude, latitude = coord_array
        if not (-180 <= longitude <= 180):
            raise ValueError(f"Invalid longitude: {longitude} (must be -180 to 180)")
        if not (-90 <= latitude <= 90):
            raise ValueError(f"Invalid latitude: {latitude} (must be -90 to 90)")

        # Add metadata for tracking
        location['metadata'] = {
            'source': 'json_import',
            'import_date': datetime.utcnow(),
            'last_updated': datetime.utcnow()
        }

        return location


# ============================================================================
# DATA LOADERS
# ============================================================================

class DataLoader:
    """
    Handles reading and parsing data from CSV and JSON files

    Provides methods to:
    - Load reviews from CSV with error handling
    - Load locations from JSON with validation
    - Handle different file encodings
    - Report validation errors
    """

    @staticmethod
    def load_csv_reviews(file_path: Path) -> List[Dict[str, Any]]:
        """
        Load and validate reviews from CSV file

        Process:
        1. Check file exists
        2. Read CSV with proper encoding
        3. Validate each row
        4. Collect validation errors
        5. Return valid reviews

        Args:
            file_path: Path to the CSV file

        Returns:
            List of validated review documents

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            ValueError: If CSV structure is invalid
        """
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        reviews = []
        errors = []

        print(f"\n📄 Reading CSV file: {file_path}")

        try:
            # Try UTF-8 encoding first
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)

                # Validate CSV headers
                expected_headers = {'Destination', 'District', 'Location_Type', 'Review'}
                actual_headers = set(reader.fieldnames)

                if not expected_headers.issubset(actual_headers):
                    missing = expected_headers - actual_headers
                    raise ValueError(f"Missing CSV columns: {missing}")

                # Read and validate each row with progress bar
                for idx, row in enumerate(tqdm(reader, desc="Loading reviews"), start=2):
                    try:
                        validated_review = DataValidator.validate_review(row)
                        reviews.append(validated_review)
                    except ValueError as e:
                        errors.append(f"Row {idx}: {str(e)}")
                        continue

        except UnicodeDecodeError:
            # Fallback to latin-1 encoding if UTF-8 fails
            print("⚠️  UTF-8 decode failed, trying latin-1 encoding...")
            with open(file_path, 'r', encoding='latin-1') as csvfile:
                reader = csv.DictReader(csvfile)
                for idx, row in enumerate(tqdm(reader, desc="Loading reviews"), start=2):
                    try:
                        validated_review = DataValidator.validate_review(row)
                        reviews.append(validated_review)
                    except ValueError as e:
                        errors.append(f"Row {idx}: {str(e)}")
                        continue

        # Report validation errors if any
        if errors:
            print(f"\n⚠️  Warning: {len(errors)} rows had validation errors:")
            for error in errors[:10]:  # Show first 10 errors
                print(f"  - {error}")
            if len(errors) > 10:
                print(f"  ... and {len(errors) - 10} more errors")

        return reviews

    @staticmethod
    def load_json_locations(file_path: Path) -> List[Dict[str, Any]]:
        """
        Load and validate locations from JSON file

        Process:
        1. Check file exists
        2. Parse JSON
        3. Validate structure (must be array)
        4. Validate each location
        5. Return valid locations

        Args:
            file_path: Path to the JSON file

        Returns:
            List of validated location documents

        Raises:
            FileNotFoundError: If JSON file doesn't exist
            json.JSONDecodeError: If JSON is malformed
        """
        if not file_path.exists():
            raise FileNotFoundError(f"JSON file not found: {file_path}")

        print(f"\n📄 Reading JSON file: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as jsonfile:
                data = json.load(jsonfile)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format: {str(e)}")

        if not isinstance(data, list):
            raise ValueError("JSON file must contain an array of locations")

        locations = []
        errors = []

        # Validate each location with progress bar
        for idx, location in enumerate(tqdm(data, desc="Loading locations"), start=1):
            try:
                validated_location = DataValidator.validate_location(location)
                locations.append(validated_location)
            except ValueError as e:
                errors.append(f"Location {idx} ({location.get('name', 'Unknown')}): {str(e)}")
                continue

        # Report validation errors if any
        if errors:
            print(f"\n⚠️  Warning: {len(errors)} locations had validation errors:")
            for error in errors[:10]:
                print(f"  - {error}")
            if len(errors) > 10:
                print(f"  ... and {len(errors) - 10} more errors")

        return locations


# ============================================================================
# MONGODB ATLAS OPERATIONS
# ============================================================================

class MongoDBManager:
    """
    Manages MongoDB Atlas connections and operations

    Handles:
    - Cloud database connections with retry logic
    - Collection creation with schema validation
    - Bulk data insertion with error handling
    - Index creation for query optimization
    - Connection lifecycle management
    """

    def __init__(self, uri: str, database_name: str):
        """
        Initialize MongoDB Atlas connection manager

        Args:
            uri: MongoDB Atlas connection string
                 Format: mongodb+srv://user:pass@cluster.mongodb.net/
            database_name: Name of the database to use
        """
        self.uri = uri
        self.database_name = database_name
        self.client = None
        self.db = None

    def connect(self):
        """
        Establish connection to MongoDB Atlas

        Process:
        1. Create MongoClient with Atlas URI
        2. Set server selection timeout
        3. Test connection with ping
        4. Get database reference

        Raises:
            ConnectionFailure: If unable to connect to MongoDB Atlas

        Note:
            Requires internet connection and valid Atlas credentials
        """
        print(f"\n🌐 Connecting to MongoDB Atlas...")
        print(f"📊 Database: {self.database_name}")

        try:
            # Create client with Atlas-specific settings
            self.client = MongoClient(
                self.uri,
                serverSelectionTimeoutMS=Config.CONNECTION_TIMEOUT,
                retryWrites=True,  # Enable retry for write operations
                w='majority'  # Write concern for data safety
            )

            # Test connection by pinging
            self.client.admin.command('ping')

            # Get database reference
            self.db = self.client[self.database_name]

            print("✅ Successfully connected to MongoDB Atlas")

        except ConnectionFailure as e:
            print(f"❌ Failed to connect to MongoDB Atlas: {str(e)}")
            print("\n🔍 Troubleshooting:")
            print("  1. Check your internet connection")
            print("  2. Verify MongoDB Atlas credentials in .env file")
            print("  3. Ensure IP address is whitelisted in Atlas")
            print("  4. Check cluster is not paused")
            raise

    def disconnect(self):
        """Close MongoDB Atlas connection"""
        if self.client:
            self.client.close()
            print("\n✅ Disconnected from MongoDB Atlas")

    def create_collections(self):
        """
        Create collections with JSON schema validation

        Schema validation ensures:
        - Required fields are present
        - Data types are correct
        - Values meet constraints
        - GeoJSON format is valid

        This enforcement happens at the database level,
        providing an additional layer of data integrity.
        """
        print("\n🔨 Creating collections with schema validation...")

        # ---- Reviews Collection Schema ----
        reviews_schema = {
            "bsonType": "object",
            "required": ["destination", "district", "location_type", "review_text"],
            "properties": {
                "destination": {
                    "bsonType": "string",
                    "description": "Name of the destination (required)"
                },
                "district": {
                    "bsonType": "string",
                    "description": "District where destination is located (required)"
                },
                "location_type": {
                    "bsonType": "string",
                    "description": "Type of location: Beaches, Historical, Nature, etc. (required)"
                },
                "review_text": {
                    "bsonType": "string",
                    "minLength": 10,
                    "description": "Review content, minimum 10 characters (required)"
                },
                "created_at": {
                    "bsonType": "date",
                    "description": "Timestamp when review was created"
                },
                "metadata": {
                    "bsonType": "object",
                    "description": "Import metadata and tracking info"
                }
            }
        }

        # ---- Locations Collection Schema ----
        locations_schema = {
            "bsonType": "object",
            "required": ["locationId", "name", "coordinates"],
            "properties": {
                "locationId": {
                    "bsonType": "string",
                    "description": "Unique identifier for location (required)"
                },
                "name": {
                    "bsonType": "string",
                    "description": "Location name (required)"
                },
                "coordinates": {
                    "bsonType": "object",
                    "required": ["type", "coordinates"],
                    "properties": {
                        "type": {
                            "enum": ["Point"],
                            "description": "GeoJSON type (must be 'Point')"
                        },
                        "coordinates": {
                            "bsonType": "array",
                            "minItems": 2,
                            "maxItems": 2,
                            "items": {
                                "bsonType": "double"
                            },
                            "description": "Array of [longitude, latitude]"
                        }
                    }
                },
                "category": {
                    "bsonType": "array",
                    "description": "Array of category tags"
                },
                "details": {
                    "bsonType": "object",
                    "description": "Detailed location information"
                },
                "logistics": {
                    "bsonType": "object",
                    "description": "Hours, fees, facilities, accessibility"
                }
            }
        }

        # Drop existing collections if they exist (fresh start)
        existing_collections = self.db.list_collection_names()

        if Config.REVIEWS_COLLECTION in existing_collections:
            print(f"  🗑️  Dropping existing '{Config.REVIEWS_COLLECTION}' collection")
            self.db[Config.REVIEWS_COLLECTION].drop()

        if Config.LOCATIONS_COLLECTION in existing_collections:
            print(f"  🗑️  Dropping existing '{Config.LOCATIONS_COLLECTION}' collection")
            self.db[Config.LOCATIONS_COLLECTION].drop()

        # Create new collections with validation
        try:
            self.db.create_collection(
                Config.REVIEWS_COLLECTION,
                validator={"$jsonSchema": reviews_schema}
            )
            print(f"  ✅ Created '{Config.REVIEWS_COLLECTION}' with schema validation")

            self.db.create_collection(
                Config.LOCATIONS_COLLECTION,
                validator={"$jsonSchema": locations_schema}
            )
            print(f"  ✅ Created '{Config.LOCATIONS_COLLECTION}' with schema validation")

        except OperationFailure as e:
            print(f"❌ Error creating collections: {str(e)}")
            raise

    def insert_reviews(self, reviews: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Insert reviews into MongoDB Atlas in batches

        Uses bulk operations for efficiency and handles partial failures.

        Args:
            reviews: List of validated review documents

        Returns:
            Dictionary with statistics: total, inserted, errors
        """
        collection = self.db[Config.REVIEWS_COLLECTION]
        total = len(reviews)
        inserted = 0
        errors = 0

        print(f"\n📝 Inserting {total:,} reviews into MongoDB Atlas...")

        # Process in batches for better performance
        for i in tqdm(range(0, total, Config.BATCH_SIZE), desc="Inserting reviews"):
            batch = reviews[i:i + Config.BATCH_SIZE]
            try:
                result = collection.insert_many(batch, ordered=False)
                inserted += len(result.inserted_ids)
            except BulkWriteError as e:
                # Even with errors, some documents may have been inserted
                inserted += e.details['nInserted']
                errors += len(e.details['writeErrors'])

        return {'total': total, 'inserted': inserted, 'errors': errors}

    def insert_locations(self, locations: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Insert locations into MongoDB Atlas in batches

        Args:
            locations: List of validated location documents

        Returns:
            Dictionary with statistics: total, inserted, errors
        """
        collection = self.db[Config.LOCATIONS_COLLECTION]
        total = len(locations)
        inserted = 0
        errors = 0

        print(f"\n📍 Inserting {total:,} locations into MongoDB Atlas...")

        # Process in batches
        for i in tqdm(range(0, total, Config.BATCH_SIZE), desc="Inserting locations"):
            batch = locations[i:i + Config.BATCH_SIZE]
            try:
                result = collection.insert_many(batch, ordered=False)
                inserted += len(result.inserted_ids)
            except BulkWriteError as e:
                inserted += e.details['nInserted']
                errors += len(e.details['writeErrors'])

        return {'total': total, 'inserted': inserted, 'errors': errors}

    def create_indexes(self):
        """
        Create indexes for optimal query performance

        Index Types:
        1. Text Indexes - Full-text search on review content
        2. Geospatial Indexes (2dsphere) - Location-based queries
        3. Single Field Indexes - Fast filtering
        4. Compound Indexes - Multi-field queries

        These indexes dramatically improve query speed for:
        - Search functionality
        - Location-based recommendations
        - Filtering by destination/district/type
        """
        print("\n🔍 Creating indexes for fast queries...")

        reviews_collection = self.db[Config.REVIEWS_COLLECTION]
        locations_collection = self.db[Config.LOCATIONS_COLLECTION]

        # ---- Reviews Collection Indexes ----

        # 1. Text index for full-text search
        print("  📝 Creating text index on review_text...")
        reviews_collection.create_index(
            [("review_text", TEXT)],
            name="review_text_index",
            default_language="english"
        )

        # 2. Index on destination for fast filtering
        print("  📍 Creating index on destination...")
        reviews_collection.create_index(
            [("destination", ASCENDING)],
            name="destination_index"
        )

        # 3. Index on district
        print("  🗺️  Creating index on district...")
        reviews_collection.create_index(
            [("district", ASCENDING)],
            name="district_index"
        )

        # 4. Index on location_type
        print("  🏷️  Creating index on location_type...")
        reviews_collection.create_index(
            [("location_type", ASCENDING)],
            name="location_type_index"
        )

        # 5. Compound index for common queries
        print("  🔗 Creating compound index (destination + location_type)...")
        reviews_collection.create_index(
            [("destination", ASCENDING), ("location_type", ASCENDING)],
            name="destination_location_type_index"
        )

        # 6. Time-based index for date queries
        print("  📅 Creating index on created_at...")
        reviews_collection.create_index(
            [("created_at", DESCENDING)],
            name="created_at_index"
        )

        # ---- Locations Collection Indexes ----

        # 1. Geospatial index for location queries
        print("  🌍 Creating 2dsphere geospatial index on coordinates...")
        locations_collection.create_index(
            [("coordinates", GEOSPHERE)],
            name="coordinates_geospatial_index"
        )

        # 2. Unique index on locationId
        print("  🆔 Creating unique index on locationId...")
        locations_collection.create_index(
            [("locationId", ASCENDING)],
            name="location_id_unique_index",
            unique=True
        )

        # 3. Index on name for lookups
        print("  🏛️  Creating index on name...")
        locations_collection.create_index(
            [("name", ASCENDING)],
            name="name_index"
        )

        # 4. Index on category array
        print("  🏷️  Creating index on category...")
        locations_collection.create_index(
            [("category", ASCENDING)],
            name="category_index"
        )

        # 5. Text index for searching names and descriptions
        print("  🔍 Creating text index on name and description...")
        locations_collection.create_index(
            [
                ("name", TEXT),
                ("details.description", TEXT),
                ("details.historicalInfo", TEXT)
            ],
            name="location_text_index",
            default_language="english"
        )

        print("  ✅ All indexes created successfully")


# ============================================================================
# STATISTICS AND REPORTING
# ============================================================================

class StatsReporter:
    """
    Generate comprehensive summary statistics

    Provides insights into:
    - Data loading success rates
    - Distribution by categories
    - Top destinations and districts
    - Index information
    """

    @staticmethod
    def generate_summary(db, review_stats: Dict, location_stats: Dict):
        """
        Generate and print detailed summary statistics

        Args:
            db: MongoDB database instance
            review_stats: Review insertion statistics
            location_stats: Location insertion statistics
        """
        print("\n" + "="*70)
        print("📊 SUMMARY STATISTICS")
        print("="*70)

        # ---- Reviews Statistics ----
        print("\n📝 REVIEWS COLLECTION:")
        print(f"  Total processed: {review_stats['total']:,}")
        print(f"  Successfully inserted: {review_stats['inserted']:,}")
        print(f"  Errors: {review_stats['errors']:,}")

        reviews_collection = db[Config.REVIEWS_COLLECTION]

        # Distribution by location type
        print("\n  Distribution by Location Type:")
        pipeline = [
            {"$group": {"_id": "$location_type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        for doc in reviews_collection.aggregate(pipeline):
            print(f"    - {doc['_id']}: {doc['count']:,}")

        # Distribution by district
        print("\n  Distribution by District:")
        pipeline = [
            {"$group": {"_id": "$district", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        for doc in reviews_collection.aggregate(pipeline):
            print(f"    - {doc['_id']}: {doc['count']:,}")

        # Top destinations
        print("\n  Top 10 Destinations by Review Count:")
        pipeline = [
            {"$group": {"_id": "$destination", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        for idx, doc in enumerate(reviews_collection.aggregate(pipeline), 1):
            print(f"    {idx}. {doc['_id']}: {doc['count']:,} reviews")

        # ---- Locations Statistics ----
        print(f"\n📍 LOCATIONS COLLECTION:")
        print(f"  Total processed: {location_stats['total']:,}")
        print(f"  Successfully inserted: {location_stats['inserted']:,}")
        print(f"  Errors: {location_stats['errors']:,}")

        locations_collection = db[Config.LOCATIONS_COLLECTION]

        # Distribution by category
        print("\n  Distribution by Category:")
        pipeline = [
            {"$unwind": "$category"},
            {"$group": {"_id": "$category", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        for doc in locations_collection.aggregate(pipeline):
            print(f"    - {doc['_id']}: {doc['count']:,}")

        # ---- Index Information ----
        print("\n🔍 INDEXES CREATED:")

        print(f"\n  Reviews Collection ({len(list(reviews_collection.list_indexes()))} indexes):")
        for index in reviews_collection.list_indexes():
            keys = list(index['key'].keys())
            print(f"    - {index['name']}: {', '.join(keys)}")

        print(f"\n  Locations Collection ({len(list(locations_collection.list_indexes()))} indexes):")
        for index in locations_collection.list_indexes():
            keys = list(index['key'].keys())
            print(f"    - {index['name']}: {', '.join(keys)}")

        print("\n" + "="*70)
        print("✅ Data loading completed successfully!")
        print("="*70 + "\n")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main execution function - orchestrates the entire data loading process

    Process Flow:
    1. Load data from CSV and JSON files
    2. Connect to MongoDB Atlas
    3. Create collections with schema validation
    4. Insert reviews with progress tracking
    5. Insert locations with progress tracking
    6. Create performance indexes
    7. Generate summary statistics

    Error Handling:
    - File not found errors
    - Connection failures
    - Validation errors
    - Unexpected exceptions
    """
    print("\n" + "="*70)
    print("🇱🇰 SRI LANKA TOUR GUIDE - MONGODB ATLAS DATA LOADER")
    print("="*70)

    mongo_manager = None

    try:
        # ---- Step 1: Load data from files ----
        print("\n[1/6] 📂 Loading data from files...")

        reviews = DataLoader.load_csv_reviews(Config.CSV_FILE)
        locations = DataLoader.load_json_locations(Config.JSON_FILE)

        # Validate we have data
        if not reviews:
            print("⚠️  Warning: No valid reviews loaded from CSV")
        if not locations:
            print("⚠️  Warning: No valid locations loaded from JSON")

        print(f"\n✅ Loaded {len(reviews):,} reviews and {len(locations):,} locations")

        # ---- Step 2: Connect to MongoDB Atlas ----
        print("\n[2/6] 🌐 Connecting to MongoDB Atlas...")

        mongo_manager = MongoDBManager(Config.MONGO_URI, Config.DATABASE_NAME)
        mongo_manager.connect()

        # ---- Step 3: Create collections ----
        print("\n[3/6] 🔨 Creating collections...")

        mongo_manager.create_collections()

        # ---- Step 4: Insert reviews ----
        print("\n[4/6] 📝 Inserting reviews...")

        review_stats = mongo_manager.insert_reviews(reviews)

        # ---- Step 5: Insert locations ----
        print("\n[5/6] 📍 Inserting locations...")

        location_stats = mongo_manager.insert_locations(locations)

        # ---- Step 6: Create indexes ----
        print("\n[6/6] 🔍 Creating indexes...")

        mongo_manager.create_indexes()

        # ---- Generate summary statistics ----
        StatsReporter.generate_summary(
            mongo_manager.db,
            review_stats,
            location_stats
        )

    except FileNotFoundError as e:
        print(f"\n❌ Error: {str(e)}")
        print("\n📁 Please ensure the following files exist:")
        print(f"  - {Config.CSV_FILE}")
        print(f"  - {Config.JSON_FILE}")
        sys.exit(1)

    except ConnectionFailure as e:
        print(f"\n❌ Error: Unable to connect to MongoDB Atlas")
        print(f"  {str(e)}")
        print("\n🔍 Please ensure:")
        print("  1. Internet connection is active")
        print("  2. MongoDB Atlas credentials are correct in .env")
        print("  3. IP address is whitelisted in Atlas Network Access")
        print("  4. Cluster is not paused")
        sys.exit(1)

    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        # ---- Cleanup ----
        if mongo_manager:
            mongo_manager.disconnect()


if __name__ == "__main__":
    main()
