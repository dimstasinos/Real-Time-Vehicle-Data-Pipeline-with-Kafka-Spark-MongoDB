from datetime import datetime, timezone
import pymongo


# Function that finds which link have the minimum vcount in a given time
def min_vcount():
    # Query
    pipeline = [
        {
            "$match": {
                "time": {"$gte": time_from, "$lte": time_to}
            }
        },
        {
            "$group": {
                "_id": "$link",
                "sumCount": {"$sum": "$vcount"}
            }
        },
        {
            "$group": {
                "_id": None,
                "minCount": {"$min": "$sumCount"}
            }
        },
        {
            "$lookup": {
                "from": "Cars_processed",
                "let": {"minCount": "$minCount"},
                "pipeline": [
                    {
                        "$match": {
                            "time": {"$gte": time_from, "$lte": time_to}
                        }
                    },
                    {
                        "$group": {
                            "_id": "$link",
                            "sumCount": {"$sum": "$vcount"}
                        }
                    },
                    {
                        "$match": {
                            '$expr': {"$eq": ["$sumCount", "$$minCount"]}
                        }
                    }
                ],
                "as": "result"
            }
        },
        {
            '$unwind': '$result'
        },
        {
            '$replaceRoot': {
                'newRoot': '$result'
            }
        }

    ]

    # Execute query and results
    result_query = list(mongoCollection.aggregate(pipeline))

    # Print the results
    if len(result_query) == 0:
        print("There are no result for this time range")
    elif len(result_query) == 1:
        print(
            f"The link with the smallest number of vehicles between {time_from} sec and {time_to} sec was"
            f" {result_query[0]['_id']} with {result_query[0]['sumCount']} vehicle(s)")
    else:
        print(f"The links with the smallest number of vehicles between {time_from} sec and {time_to} sec were "
              f"({result_query[0]['sumCount']} vehicle(s)):")

        for row in result_query:
            print(f"{row['_id']} ", end='')

        print("\n")


# Function that finds which link have the maximum vspeed in a given time
def max_vspeed():
    # Query
    pipeline = [
        {
            "$match": {
                "time": {"$gte": date_from, "$lte": date_to}
            }
        },
        {
            "$group": {
                "_id": "$link",
                "avgSpeed": {"$avg": "$speed"}
            }
        },
        {
            "$group": {
                "_id": None,
                "maxSpeed": {"$max": "$avgSpeed"}
            }
        },
        {
            "$lookup": {
                "from": "Cars_raw",
                "let": {"maxSpeed": "$maxSpeed"},
                "pipeline": [
                    {
                        "$match": {
                            "time": {"$gte": date_from, "$lte": date_to}
                        }
                    },
                    {
                        "$group": {
                            "_id": "$link",
                            "avgSpeed": {"$avg": "$speed"}
                        }
                    },
                    {
                        "$match": {
                            '$expr': {"$eq": ["$avgSpeed", "$$maxSpeed"]}
                        }
                    }
                ],
                "as": "result"
            }
        },
        {
            '$unwind': '$result'
        },
        {
            '$replaceRoot': {
                'newRoot': '$result'
            }
        }
    ]

    # Execute query and result
    result_query = list(mongoCollectionRaw.aggregate(pipeline))

    # Print the results
    if len(result_query) == 0:
        print("There are no result for this time range")
    elif len(result_query) == 1:
        print(
            f"The link with the highest average speed between {date_from} sec and {date_to} sec was"
            f" {result_query[0]['_id']} with avgSpeed {result_query[0]['avgSpeed']}")
    else:
        print(f"The links with the highest average speed between {date_from} sec and {date_to} sec were"
              f" (avgSpeed: {result_query[0]['avgSpeed']}):")

        for row in result_query:
            print(f"{row['_id']} ", end='')

        print("\n")


# Function tha finds which car have done the longest route
def longest_route():
    # Query
    pipeline = [
        {
            "$match": {
                "time": {"$gte": date_from, "$lte": date_to}
            }
        },
        {
            "$group": {
                "_id": "$name",
                "link": {"$addToSet": "$link"},
                "position": {"$push": "$position"},
            }
        },
        {
            "$addFields": {
                "link_size": {"$size": "$link"},
                "pos_size": {"$size": "$position"}
            }
        },
        {
            "$addFields": {
                "route_distance": {
                    "$cond": {
                        "if": {"$eq": ["$link_size", 1]},
                        "then": {
                            "$cond": {
                                "if": {"$eq": ["$pos_size", 1]},
                                "then": 0,
                                "else": {
                                    "$subtract": [{"$arrayElemAt": ["$position", -1]},
                                                  {"$arrayElemAt": ["$position", 0]}]
                                }
                            }
                        },
                        "else": {
                            "$add": [
                                {"$subtract": [500, {"$arrayElemAt": ["$position", 0]}]},
                                {"$multiply": [{"$subtract": ["$link_size", 2]}, 500]},
                                {"$arrayElemAt": ["$position", -1]}
                            ]
                        }
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "name": "$_id",
                "route_distance": 1
            }
        },
        {
            "$group": {
                "_id": None,
                "max_distance": {"$max": "$route_distance"},
                "route_car": {"$push": {"name": "$name", "route_distance": "$route_distance"}}
            }
        },
        {"$unwind": "$route_car"},
        {
            "$match": {
                "$expr": {"$eq": ["$max_distance", "$route_car.route_distance"]}
            }
        },
        {
            "$project": {
                "_id": 0,
                "name": "$route_car.name",
                "route_distance": "$route_car.route_distance"
            }
        }
    ]

    # Execute query and result
    result_query = list(mongoCollectionRaw.aggregate(pipeline))

    # Print the results
    if len(result_query) == 0:
        print("There are no result for this time range")
    elif len(result_query) == 1:
        print(
            f"The car with the biggest route distance between {date_from} and {date_to} was"
            f" {result_query[0]['name']} with {result_query[0]['route_distance']}")
    else:
        print(f"The cars with the biggest route distance between ({date_from}) and ({date_to}) were"
              f" (distance: {result_query[0]['route_distance']}):")

        for row in result_query:
            print(f"Car name: {row['name']}")

        print("\n")


if __name__ == '__main__':
    # Time for the query
    time_from = 200
    time_to = 500
    date_from = datetime(2024, 6, 21, 16, 22, 0)
    date_to = datetime(2024, 6, 21, 16, 24, 5)

    # Connect to mongodb database and collection
    mongoClient = pymongo.MongoClient("mongodb://localhost:27017/")
    mongoDatabase = mongoClient['Big_Data_Project']  # Database
    mongoCollection = mongoDatabase["Cars_processed"]  # Processed collection
    mongoCollectionRaw = mongoDatabase["Cars_raw"]  # Raw data collection

    # Queries
    min_vcount()
    max_vspeed()
    longest_route()
