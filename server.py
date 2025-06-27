import os
from flask import Flask, jsonify, make_response
from flask_cors import CORS
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import json
import csv
from datetime import datetime
import time
import hashlib


app = Flask(__name__)
CORS(app) # allow all origins by default; in production, restrict to YOUR Vercel domain

# Cache configuration
CACHE = {}
CACHE_TTL = 3600  # 1 hour in seconds
CACHE_MAX_SIZE = 100  # Maximum number of cached items

# 1) Configure your MongoDB URI (local or Atlas).
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
client.admin.command('ping')
db = client.get_database(name="WPTable")

# 2) Collections
win_col = db["wins"]
prob_col = db['Probabilities']
delim_col = db["Delim"]
matches_col = db["matches"]  # Add matches collection

print(f"Win documents: {win_col.count_documents({})}")
print(f"Delim documents: {delim_col.count_documents({})}")
print(f"Matches documents: {matches_col.count_documents({})}")

# 3) We know exactly which ranks exist and in which order.
RANK_ORDER = [str(i) for i in range(1, 21)] + ["unranked"] # ["1","2",…,"20","unranked"]


with open("mens_waterpolo_rankings.json", "r", encoding="utf-8") as f:
    rankings = json.load(f)

# Manual team name mappings dictionary
TEAM_NAME_TO_ID = {
    'USC': 1,
    'UCLA': 2,
    'UC Berkeley': 3,
    'Stanford': 4,
    'California Baptist': 5,
    'Long Beach State': 6,
    'LMU': 7,
    'Pepperdine': 8,
    'San Jose State': 9,
    'Santa Clara': 10,
    'UC Davis': 11,
    'UC Irvine': 12,
    'UC San Diego': 13,
    'UCSB': 14,
    'George Washington': 15,
    'Navy': 16,
    'Harvard': 17,
    'Princeton': 18,
    'Fordham': 19,
    'Iona': 20,
    'Saint Francis': 21,
    'Wagner': 22,
    'Bucknell': 23,
    'La Salle': 24,
    'Brown': 25,
    'Pacific': 26,
    'Air Force': 27,
    'UC-San Diego': 13,
    'University of California Santa Barbara': 14,
    'California': 3,
    'Princeton University': 18,
    'Cal Baptist': 5,
    'Long Beach St.': 6,
    'San Jose State University': 9,
    'California Baptist University': 5,
    'Bucknell University': 23,
    'UCSD': 13,
    'Brown University': 25,
    'San Jose St.': 9,
    'University of Southern California': 1,
    'Fordham University': 19,
    'Stanford University': 4,
    'Pepperdine University': 8,
    'Wagner College': 22,
    'Santa Clara University': 10,
    'Long Beach': 6,
    'Cal': 3,
    'University of the Pacific': 26,
    'UC Santa Barbara': 14,
    'Long Beach State University': 6,
    'University of California-Los Angeles': 2,
    'University of California-Santa Barbara': 14,
    'University of California-San Diego': 13,
    'University of California-Davis': 11,
    'George Washington University': 15,
    'United States Naval Academy': 16,
    'United States Air Force Academy': 27,
    'Harvard University': 17,
    'Loyola Marymount University': 7,
    'University of California-Irvine': 12,
    'University of California': 3,
    'Long Beach St': 6,
    'SJSU Spartans': 9,
    'UCI': 12,
    'CBU': 5,
    'Iona University': 20,
    'southern-california': 1,
    'San José State': 9,
    'Iona College': 20,
    'Loyola Marymount': 7,
    'University of California Irvine': 12,
    'University of California Davis': 11,
    'Loyola-Marymount': 7,
    'UC-Santa Barbara': 14,
    'Southern California': 1,
    'Air Force Academy': 27,
    'Southern Cal': 1,
    'UC-Irvine': 12,
    'Cal-Baptist': 5,
    'Claremont-Mudd-Scripps Colleges': 28,
    'Concordia Univeristy (Calif.)': 29,
    'Concordia University': 29,
    'Concordia University (Calif.)': 29,
    'Johns Hopkins University': 30,
    'Massachusetts Institute of Technology': 31,
    'Mercyhurst University': 32,
    'Pomona Pitzer Colleges': 33,
    'Pomona-Pitzer Colleges': 33,
    'Salem University': 34,
    'US Air Force Academy': 27,
    'University of Redlands': 35,
    'Whittier College': 36
}

# Reverse mapping: ID to canonical team name
ID_TO_TEAM_NAME = {
    1: 'USC',
    2: 'UCLA',
    3: 'UC Berkeley',
    4: 'Stanford',
    5: 'California Baptist',
    6: 'Long Beach State',
    7: 'LMU',
    8: 'Pepperdine',
    9: 'San Jose State',
    10: 'Santa Clara',
    11: 'UC Davis',
    12: 'UC Irvine',
    13: 'UC San Diego',
    14: 'UCSB',
    15: 'George Washington',
    16: 'Navy',
    17: 'Harvard',
    18: 'Princeton',
    19: 'Fordham',
    20: 'Iona',
    21: 'Saint Francis',
    22: 'Wagner',
    23: 'Bucknell',
    24: 'La Salle',
    25: 'Brown',
    26: 'Pacific',
    27: 'Air Force',
    28: 'Claremont-Mudd-Scripps',
    29: 'Concordia University',
    30: 'Johns Hopkins',
    31: 'MIT',
    32: 'Mercyhurst',
    33: 'Pomona-Pitzer',
    34: 'Salem University',
    35: 'University of Redlands',
    36: 'Whittier College'
}

print(f"Loaded {len(TEAM_NAME_TO_ID)} team name mappings")

def normalize_team_name(team_name):
    """Convert team name to standardized form using mappings"""
    if not team_name:
        return None
        
    team_name = team_name.strip()
    
    # Direct mapping
    if team_name in TEAM_NAME_TO_ID:
        team_id = TEAM_NAME_TO_ID[team_name]
        return ID_TO_TEAM_NAME.get(team_id, team_name)
    
    # Case-insensitive mapping
    team_name_lower = team_name.lower()
    for mapped_name, team_id in TEAM_NAME_TO_ID.items():
        if mapped_name.lower() == team_name_lower:
            return ID_TO_TEAM_NAME.get(team_id, team_name)
    
    # Return original if no mapping found
    return team_name

def convert_team_name_to_id(team_name):
    """Convert team name to team ID"""
    if not team_name:
        return None
        
    team_name = team_name.strip()
    
    # Direct mapping
    if team_name in TEAM_NAME_TO_ID:
        return TEAM_NAME_TO_ID[team_name]
    
    # Case-insensitive mapping
    team_name_lower = team_name.lower()
    for mapped_name, team_id in TEAM_NAME_TO_ID.items():
        if mapped_name.lower() == team_name_lower:
            return team_id
    
    # Return None if no mapping found
    return None

def find_teams_in_rankings(target_teams):
    """Find all team IDs in rankings data that match the target teams"""
    target_team_ids = set()
    
    # Convert team names to IDs
    for team in target_teams:
        team_id = convert_team_name_to_id(team)
        if team_id:
            target_team_ids.add(team_id)
    
    return list(target_team_ids)

# Cache management functions
def cache_key_generator(*args):
    """Generate a cache key from arguments"""
    key_string = "_".join(str(arg) for arg in args)
    return hashlib.md5(key_string.encode()).hexdigest()

def get_from_cache(key):
    """Get data from cache if it exists and is not expired"""
    if key in CACHE:
        data, timestamp = CACHE[key]
        if time.time() - timestamp < CACHE_TTL:
            return data
        else:
            # Remove expired entry
            del CACHE[key]
    return None

def set_cache(key, data):
    """Set data in cache with timestamp"""
    # Implement simple LRU by removing oldest entries if cache is full
    if len(CACHE) >= CACHE_MAX_SIZE:
        # Remove the oldest entry
        oldest_key = min(CACHE.keys(), key=lambda k: CACHE[k][1])
        del CACHE[oldest_key]
    
    CACHE[key] = (data, time.time())

def add_cache_headers(response, max_age=3600):
    """Add cache headers to response"""
    response.headers['Cache-Control'] = f'public, max-age={max_age}, s-maxage={max_age}, stale-while-revalidate=7200'
    response.headers['ETag'] = hashlib.md5(response.get_data()).hexdigest()
    return response

def fetch_collection_as_aligned_list(collection, is_float):
    # 1) Fetch all documents (exclude _id)
    docs = list(collection.find({}, {"_id": 0}))
    
    # 2) Build a map whose keys are always the string version of Rank:
    doc_map = {}
    for doc in docs:
        raw_rank = doc.get("Rank")
        # Convert to lowercase string, trimmed of whitespace:
        normalized_rank = str(raw_rank).strip().lower()
        doc_map[normalized_rank] = doc
    
    aligned = []
    for rank in RANK_ORDER: # RANK_ORDER is ["1","2",...,"20","unranked"]
        doc = doc_map.get(rank)
        if doc is None:
            # If a rank is missing, create empty row with nulls
            clean_row = {"rank": rank}
            for header in RANK_ORDER:
                clean_row[header] = None
            aligned.append(clean_row)
            continue
        
        clean_row = {"rank": rank}
        for key, val in doc.items():
            if key == "Rank":
                continue
            if val is None or val == "":
                clean_row[key] = None
            else:
                clean_row[key] = float(val) if is_float else int(val)
        aligned.append(clean_row)
    
    return aligned

@app.route("/api/matrix", methods=["GET"])
def get_matrix():
    try:
        # Check cache first
        cache_key = cache_key_generator("matrix", "v1")
        cached_data = get_from_cache(cache_key)
        
        if cached_data:
            print("Serving matrix data from cache")
            response = make_response(jsonify(cached_data))
            return add_cache_headers(response), 200
        
        print("Fetching matrix data from database")
        delim_data = fetch_collection_as_aligned_list(delim_col, is_float=False) # Game counts
        prob_data = fetch_collection_as_aligned_list(prob_col,is_float=True)
        
        headers = RANK_ORDER.copy() # ["1","2",...,"20","unranked"]
        
        result_data = {
            "headers": headers,
            "probData": prob_data, # Changed from "winData" to "probData"
            "delimData": delim_data
        }
        
        # Cache the result
        set_cache(cache_key, result_data)
        
        print(f"Returning {len(prob_data)} probability rows and {len(delim_data)} delim rows")
        response = make_response(jsonify(result_data))
        return add_cache_headers(response), 200
        
    except Exception as e:
        print(f"Error in /api/matrix: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/matches/<row_rank>/<col_rank>", methods=["GET"])
def get_matches(row_rank, col_rank):
    """Get matches between two specific ranks"""
    try:
        # Check cache first
        cache_key = cache_key_generator("matches", row_rank, col_rank)
        cached_data = get_from_cache(cache_key)
        
        if cached_data:
            print(f"Serving matches data for {row_rank}_{col_rank} from cache")
            response = make_response(jsonify(cached_data))
            return add_cache_headers(response), 200
        
        print(f"Fetching matches data for {row_rank}_{col_rank} from database")
        
        # Create the key for matches lookup
        # Try both directions since matches can be stored as "3_9" or "9_3"
        key1 = f"{int(row_rank)-1}_{int(col_rank)-1}"

        # Look for matches document
        matches_doc = matches_col.find({},{"_id": 0})
        games = list(matches_doc)[0][key1]

        result_data = {
            "matches": games,
            "count": len(games),
            "row_rank": row_rank,
            "col_rank": col_rank
        }
        
        # Cache the result
        set_cache(cache_key, result_data)
        
        response = make_response(jsonify(result_data))
        return add_cache_headers(response), 200
        
    except Exception as e:
        print(f"Error in /api/matches/{row_rank}/{col_rank}: {e}")
        return jsonify({"error": str(e)}), 500
    

@app.route("/rankings/<team_names>/<start_date>/<end_date>", methods=["GET"])
def get_team_ranking_history(team_names, start_date, end_date):
    try:
        # Check cache first
        cache_key = cache_key_generator("rankings", team_names, start_date, end_date)
        cached_data = get_from_cache(cache_key)
        
        if cached_data:
            print(f"Serving ranking history for {team_names} from cache")
            response = make_response(jsonify(cached_data))
            return add_cache_headers(response), 200
        
        print(f"Fetching ranking history for {team_names} from database")
        
        # Convert string dates to datetime objects for comparison
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Parse team names (comma-separated) and convert to team IDs
        requested_teams = [name.strip() for name in team_names.split(',')]
        target_team_ids = []
        
        for team_name in requested_teams:
            team_id = convert_team_name_to_id(team_name)
            if team_id:
                target_team_ids.append(team_id)
            else:
                # Try to parse as direct ID if it's a number
                try:
                    team_id = int(team_name)
                    target_team_ids.append(team_id)
                except ValueError:
                    print(f"Warning: Could not map team name '{team_name}' to ID")
        
        print(f"Looking for team IDs: {target_team_ids}")
        
        history = []
        for date_str, ranking_list in rankings.items():
            # Parse date string to datetime object
            try:
                current_date = datetime.strptime(date_str.split('-')[0], "%m/%d/%Y")
            except ValueError:
                continue  # Skip invalid date formats
            
            # Check if current_date is within the specified range
            if start_dt <= current_date <= end_dt:
                for team in ranking_list:
                    # Since JSON now uses team_id instead of team_name
                    team_id = team.get('team_id')
                    team_rank = team.get('ranking')
                    
                    if team_id in target_team_ids:
                        # Get canonical team name from ID
                        canonical_name = ID_TO_TEAM_NAME.get(team_id, f"Team {team_id}")
                        
                        history.append({
                            "team_id": team_id,
                            "team_name": canonical_name,
                            "date": current_date.strftime("%Y-%m-%d"),
                            "rank": team_rank
                        })
        
        # Sort by date and team name
        history.sort(key=lambda x: (x['date'], x['team_name']))
        
        result_data = {
            "data": history,
            "count": len(history),
            "requested_teams": requested_teams,
            "target_team_ids": target_team_ids,
            "date_range": {
                "start": start_date,
                "end": end_date
            }
        }
        
        # Cache the result
        set_cache(cache_key, result_data)
        
        response = make_response(jsonify(result_data))
        return add_cache_headers(response), 200
        
    except Exception as e:
        print(f"Error in /rankings/{team_names}/{start_date}/{end_date}: {e}")
        return jsonify({"error": str(e)}), 500



@app.route("/api/health", methods=["GET"])
def health_check():
    """Simple health check endpoint"""
    return jsonify({"status": "healthy", "message": "Flask server is running"}), 200

@app.route("/api/cache/info", methods=["GET"])
def cache_info():
    """Get cache statistics"""
    cache_stats = {
        "cache_size": len(CACHE),
        "max_cache_size": CACHE_MAX_SIZE,
        "cache_ttl_seconds": CACHE_TTL,
        "cached_keys": list(CACHE.keys()) if len(CACHE) < 20 else f"{len(CACHE)} keys (too many to list)"
    }
    return jsonify(cache_stats), 200

@app.route("/api/cache/clear", methods=["POST"])
def clear_cache():
    """Clear all cache entries"""
    global CACHE
    old_size = len(CACHE)
    CACHE.clear()
    return jsonify({
        "message": f"Cache cleared successfully. Removed {old_size} entries.",
        "cache_size": len(CACHE)
    }), 200

@app.route("/api/teams", methods=["GET"])
def get_available_teams():
    """Get list of all available teams and mappings"""
    try:
        # Check cache first
        cache_key = cache_key_generator("available_teams", "v1")
        cached_data = get_from_cache(cache_key)
        
        if cached_data:
            print("Serving available teams from cache")
            response = make_response(jsonify(cached_data))
            return add_cache_headers(response), 200
        
        print("Fetching available teams from database")
        
        # Get all unique team IDs from rankings
        all_team_ids = set()
        
        for date_str, ranking_list in rankings.items():
            for team in ranking_list:
                team_id = team.get('team_id')
                if team_id:
                    all_team_ids.add(team_id)
        
        # Create list of teams with their names and IDs
        teams_list = []
        for team_id in sorted(all_team_ids):
            team_name = ID_TO_TEAM_NAME.get(team_id, f"Team {team_id}")
            teams_list.append({
                "team_id": team_id,
                "team_name": team_name
            })
        
        result_data = {
            "teams": teams_list,
            "team_count": len(teams_list),
            "name_mappings": TEAM_NAME_TO_ID,
            "id_mappings": ID_TO_TEAM_NAME
        }
        
        # Cache the result
        set_cache(cache_key, result_data)
        
        response = make_response(jsonify(result_data))
        return add_cache_headers(response), 200
        
    except Exception as e:
        print(f"Error in /api/teams: {e}")
        return jsonify({"error": str(e)}), 500

def warm_cache():
    """Pre-populate cache with commonly requested data"""
    print("Warming up cache...")
    
    try:
        # Warm up matrix data
        cache_key = cache_key_generator("matrix", "v1")
        if not get_from_cache(cache_key):
            delim_data = fetch_collection_as_aligned_list(delim_col, is_float=False)
            prob_data = fetch_collection_as_aligned_list(prob_col, is_float=True)
            headers = RANK_ORDER.copy()
            
            result_data = {
                "headers": headers,
                "probData": prob_data,
                "delimData": delim_data
            }
            set_cache(cache_key, result_data)
            print("Matrix data cached")
        
        # Warm up some common matches (rank 1-5 vs rank 1-5)
        for i in range(1, 6):
            for j in range(1, 6):
                if i != j:  # Don't cache same rank vs same rank
                    cache_key = cache_key_generator("matches", str(i), str(j))
                    if not get_from_cache(cache_key):
                        try:
                            key1 = f"{i-1}_{j-1}"
                            matches_doc = matches_col.find({}, {"_id": 0})
                            games = list(matches_doc)[0][key1]
                            
                            result_data = {
                                "matches": games,
                                "count": len(games),
                                "row_rank": str(i),
                                "col_rank": str(j)
                            }
                            set_cache(cache_key, result_data)
                        except:
                            pass  # Skip if data doesn't exist
        
        print(f"Cache warming completed. Cache size: {len(CACHE)}")
        
    except Exception as e:
        print(f"Error warming cache: {e}")

# Warm cache on startup
def startup():
    warm_cache()

if __name__ == "__main__":
    # Warm cache before starting the server
    startup()
    # When you run locally: python app.py → listens on http://127.0.0.1:5001
    app.run(host="0.0.0.0", port=5001, debug=True)

