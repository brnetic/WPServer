import os
from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from pymongo.server_api import ServerApi

app = Flask(__name__)
CORS(app) # allow all origins by default; in production, restrict to YOUR Vercel domain

# 1) Configure your MongoDB URI (local or Atlas).
MONGO_URI = "mongodb+srv://lukabrnetic:Lukaerik1@waterpolorankings.hrnr828.mongodb.net/"
client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
client.admin.command('ping')
db = client.get_database(name="WPTable")

# 2) Collections
win_col = db["wins"]
delim_col = db["Delim"]
matches_col = db["matches"]  # Add matches collection

print(f"Win documents: {win_col.count_documents({})}")
print(f"Delim documents: {delim_col.count_documents({})}")
print(f"Matches documents: {matches_col.count_documents({})}")

# 3) We know exactly which ranks exist and in which order.
RANK_ORDER = [str(i) for i in range(1, 21)] + ["unranked"] # ["1","2",…,"20","unranked"]

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
        # 4) Fetch both collections
        win_data = fetch_collection_as_aligned_list(win_col, is_float=False) # Raw win counts
        delim_data = fetch_collection_as_aligned_list(delim_col, is_float=False) # Game counts
        
        # 5) Calculate probabilities by dividing wins by total games
        prob_data = []
        for i, win_row in enumerate(win_data):
            delim_row = delim_data[i] if i < len(delim_data) else {}
            prob_row = {"rank": win_row["rank"]}
            
            for header in RANK_ORDER:
                wins = win_row.get(header)
                games = delim_row.get(header)
                
                if wins is None or games is None or games == 0:
                    prob_row[header] = None
                else:
                    prob_row[header] = wins / games # Calculate probability
            
            prob_data.append(prob_row)
        
        # 6) Headers (column names for the matrix)
        headers = RANK_ORDER.copy() # ["1","2",...,"20","unranked"]
        
        print(f"Returning {len(prob_data)} probability rows and {len(delim_data)} delim rows")
        return jsonify({
            "headers": headers,
            "probData": prob_data, # Changed from "winData" to "probData"
            "delimData": delim_data
        }), 200
        
    except Exception as e:
        print(f"Error in /api/matrix: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/matches/<row_rank>/<col_rank>", methods=["GET"])
def get_matches(row_rank, col_rank):
    """Get matches between two specific ranks"""
    try:
        # Normalize rank strings (handle "unranked" case)

        
        # Create the key for matches lookup
        # Try both directions since matches can be stored as "3_9" or "9_3"
        key1 = f"{int(row_rank)-1}_{int(col_rank)-1}"

        # Look for matches document
        matches_doc = matches_col.find({},{"_id": 0})
        games = list(matches_doc)[0][key1]

        
        if not matches_doc:
            return jsonify({
                "matches": [],
                "message": f"No matches found between rank {row_rank} and rank {col_rank}"
            }), 200
        
        # Extract matches from the document
        # The document structure should be like: {"3_9": [match1, match2, ...]}

        
        return jsonify({
            "matches": games,
            "count": len(games),
            "row_rank": row_rank,
            "col_rank": col_rank
        }), 200
        
    except Exception as e:
        print(f"Error in /api/matches/{row_rank}/{col_rank}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/health", methods=["GET"])
def health_check():
    """Simple health check endpoint"""
    return jsonify({"status": "healthy", "message": "Flask server is running"}), 200

if __name__ == "__main__":
    # When you run locally: python app.py → listens on http://127.0.0.1:5001
    app.run(host="0.0.0.0", port=5001, debug=True)