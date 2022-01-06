import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pprint import pprint
import decimal
from bson.decimal128 import Decimal128, create_decimal128_context

uri = "mongodb://cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/aggregations?replicaSet=Cluster0-shard-0"


async def test():
    client = AsyncIOMotorClient(
        uri,
        ssl=True,
        username="m121",
        password="aggregations",
        authSource="admin",
    )
    async with await client.start_session() as session:
        result = await client.list_database_names()
        for i in result:
            print(i)
        db = client.get_database("aggregations")
        # result = await db.list_collection_names(session=session)
        # for i in result:
        #     print(i)

        # result = await db["movies"].find_one({}, session=session)
        # pprint(result)
        # async for i in result:
        #     pprint(i)
        # pipeline = [{
        #     "$match": {
        #         "imdb.rating": {
        #             "$gte": 7
        #         },
        #         "genres": {
        #             "$nin": ["Crime", "Horror"]
        #         },
        #         "rated": {
        #             "$in": ["PG", "G"]
        #         },
        #         "languages": {
        #             "$all": ["English", "Japanese"]
        #         }
        #     }
        # }, {
        #     "$project": {
        #         "_id": False,
        #         "title": True,
        #         "rated": True
        #     }
        # }]

        # pipeline = [
        #     {
        #         "$match": {
        #             "title": "Life Is Beautiful"
        #         }
        #     },
        #     {
        #         "$project": {
        #             "writers": {
        #                 "$map": {
        #                     "input": "$writers",
        #                     "as": "writer",
        #                     "in": {
        #                         "$arrayElemAt": [{
        #                             "$split": ["$$writer", "("]
        #                         },
        #                             0
        #                         ]
        #                     }
        #                 }
        #             },
        #             "_id": False,
        #             "title": True,
        #             "cast": True
        #         }
        #     }
        # ]
        collections = await db.list_collection_names()
        for i in collections:
            print(i)
        favorites = [
            "Sandra Bullock",
            "Tom Hanks",
            "Julia Roberts",
            "Kevin Spacey",
            "George Clooney"
        ]
        pipeline = [
            {
                "$match": {
                    "cast": {
                        "$elemMatch": {
                            "$exists": True
                        }
                    },
                    "directors": {
                        "$elemMatch": {
                            "$exists": True
                        }
                    },
                    "writers": {
                        "$elemMatch": {
                            "$exists": True
                        }
                    }
                }
            },
            {
                "$project": {
                    "temp": {
                        "$setIntersection": [
                            "$cast",
                            "$directors",
                            {
                                "$map": {
                                    "input": "$writers",
                                    "as": "writer",
                                    "in": {
                                        "$arrayElemAt": [{
                                            "$split": ["$$writer", "("]
                                        },
                                            0
                                        ]
                                    }
                                }
                            },
                        ]
                    },
                    "cast": True,
                    "directors": True,
                    "writers": True,
                }
            },
            {
                "$match": {
                    # "cast": {
                    #     "$exists": True
                    # },
                    # "writers": {
                    #     "$exists": True
                    # },
                    # "directors": {
                    #     "$exists": True
                    # },
                    "temp": {
                        "$not": {
                            "$size": 0
                        },
                        # "$ne": None
                    },
                }
            },
            {
                "$count": "number"
            }
            # {
            #     "$count": "labors of love"
            # }
        ]
        pipeline = [
            {
                "$match": {
                    "cast": {
                        "$elemMatch": {
                            "$exists": True
                        }
                    },
                    "directors": {
                        "$elemMatch": {
                            "$exists": True
                        }
                    },
                    "writers": {
                        "$elemMatch": {
                            "$exists": True
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": False,
                    "cast": True,
                    "directors": True,
                    "writers": {
                        "$map": {
                            "input": "$writers",
                            "as": "writer",
                            "in": {
                                "$arrayElemAt": [
                                    {
                                        "$split": ["$$writer", "("]
                                    },
                                    0
                                ]
                            }
                        }
                    }
                }
            },
            {
                "$project": {
                    "labor_of_love": {
                        "$gt": [
                            {
                                "$size": {
                                    "$setIntersection": ["$cast", "$directors", "$writers"]
                                }
                            },
                            0
                        ]
                    }
                }
            },
            {
                "$match": {
                    "labor_of_love": True
                }
            },
            {
                "$count": "labor_of_love"
            }
        ]
        pipeline = [
            {
                "$match": {
                    "countries": {
                        "$elemMatch": {
                            "$eq": "USA"
                        }
                    },
                    "tomatoes.viewer.rating": {
                        "$gte": 3
                    }
                }
            },
            {
                "$project": {
                    "_id": False,
                    "title": True,
                    "rating": "$tomatoes.viewer.rating",
                    "temp": {
                        "$setIntersection": [
                            favorites,
                            "$cast"
                        ]
                    }
                }
            },
            {
                "$match": {
                    "temp": {
                        "$elemMatch": {
                            "$exists": True
                        }
                    }
                }
            },
            {
                "$project": {
                    # "_id": False,
                    "title": True,
                    # "rating": "$tomatoes.viewer.rating",
                    "rating": True,
                    "num_favs": {
                        "$size": "$temp"
                    }
                }
            },
            {
                "$sort": {
                    "num_favs": -1,
                    "rating": -1,
                    "title": -1
                }
            },
            {
                "$limit": 25
            }
        ]
        pipeline = [
            {
                "$limit": 10
            }
        ]
        pipeline = [
            {
                "$match": {
                    "imdb.rating": {
                        "$gte": 1
                    },
                    "imdb.votes": {
                        "$gte": 1
                    },
                    "year": {
                        "$gte": 1990
                    },
                    "languages": {
                        "$elemMatch": {
                            "$eq": "English"
                        }
                    }
                },
            },
            {
                "$project": {
                    "_id": False,
                    "normalized_rating": {
                        "$avg": [
                            {"$add": [
                                1,
                                {
                                    "$multiply": [
                                        9,
                                        {
                                            "$divide": [
                                                {"$subtract": ["$imdb.votes", 5]},
                                                {"$subtract": [1521105, 5]}
                                            ]
                                        }
                                    ]
                                }
                            ]},
                            "$imdb.rating"
                        ]
                    },
                    "title": True
                }
            },
            {
                "$sort": {
                    "normalized_rating": 1
                }
            },
            {
                "$limit": 1
            }
        ]
        pipeline = [
            {
                "$project": {
                    "_id": 0,
                    "max_high": {
                        "$reduce": {
                            # $reduce的输入
                            "input": "$trends",
                            # $reduce的初始值，即从initialValue开始进行运算
                            # -Infinity
                            "initialValue": 0,
                            "in": {
                                "$cond": [
                                    {
                                        "$gt": ["$$this.avg_high_tmp", "$$value"]
                                    },
                                    "$$this.avg_high_tmp",
                                    "$$value"
                                ]
                            }
                        }
                    }
                }
            }
        ]
        pipeline = [
            {
                "$match": {
                    "awards": {
                        "$exists": True
                    }
                }
            },
            {
                "$match": {
                    "awards": {
                        "$regex": r"^Won \d{1,2} Oscars?"
                    }
                }
            },

            {
                "$group": {
                    "_id": None,
                    "highest_rating": {
                        "$max": "$imdb.rating"
                    },
                    "lowest_rating": {
                        "$min": "$imdb.rating"
                    },
                    "average_rating": {
                        "$avg": "$imdb.rating"
                    },
                    "deviation": {
                        "$stdDevPop": "$imdb.rating"
                    },
                }
            }
        ]
        pipeline = [
            {
                "$match": {
                    "languages": {
                        "$elemMatch": {
                            "$eq": "English"
                        }
                    },
                    "cast": {
                        "$elemMatch": {
                            "$exists": True
                        }
                    }
                }
            },
            {
                "$unwind": "$cast"
            },
            {
                "$group": {
                    "_id": "$cast",
                    "numFilms": {
                        "$sum": 1
                    },
                    "average": {
                        "$avg": "$imdb.rating"
                    }
                }
            },
            {
                "$sort": {
                    "numFilms": -1
                }
            },
            {
                # "$match": {
                #     ""
                # }
                "$limit": 10
            }
        ]
        pipeline = [
            # {
            #     "$match": {
            #         "airlines": {
            #             "$elemMatch": {
            #                 "$eq": "Air Burkina"
            #             }
            #         }
            #     }
            # },
            {
                "$match": {
                    # "airline.name": "China Eastern Airlines",
                    "airplane": {
                        "$regex": r"747|380"
                        # "$eq": "380"
                    }
                }
            },
            {
                "$lookup": {
                    "from": "air_alliances",
                    "localField": "airline.name",
                    "foreignField": "airlines",
                    "as": "alliances"
                }
            },
            {
                "$unwind": {
                    "path": "$alliances",
                    # "preserveNullAndEmptyArrays": True,
                }
            },
            # {
            #     "$project": {
            #         "_id": False,
            #         "alliances": "$alliances.name"
            #     }
            # },
            {
                "$group": {
                    "_id": "$alliances.name",
                    "num_airlines": {
                        "$sum": 1
                    }
                }
            },
            # {
            #     "$limit": 10
            # }
            # {
            #     "$count": "num"
            # }
        ]
        decimal128_ctx = create_decimal128_context()
        with decimal.localcontext(decimal128_ctx) as ctx:
            Decimal128(ctx.create_decimal("1E6145"))
        infinity = Decimal128("Infinity")
        pipeline = [
            {
                "$bucket": {
                    "groupBy": "$airplane",
                    "boundaries": ["319", "A81", "CR2"],
                    "default": "Others"
                }
            }
            # {
            #     "$limit": 10
            # }
        ]
        pipeline = [
            {
                "$match": {
                    "metacritic": {"$gte": 0},
                    "imdb.rating": {"$gte": 0}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "metacritic": 1,
                    "imdb": 1,
                    "title": 1
                }
            },
            {
                "$facet": {
                    "top_metacritic": [
                        {
                            "$sort": {
                                "metacritic": -1,
                                "title": 1
                            }
                        },
                        {
                            "$limit": 10
                        },
                        {
                            "$project": {"title": True}
                        }
                    ],
                    "top_imdb": [
                        {
                            "$sort": {
                                "imdb.rating": -1,
                                "title": 1
                            }
                        },
                        {
                            "$limit": 10
                        },
                        {
                            "$project": {
                                "title": True
                            }
                        }
                    ]
                }
            },
            {
                "$project": {
                    "movies_in_both": {
                        "$setIntersection": ["$top_metacritic", "$top_imdb"]
                    }
                }
            }
        ]
        # cursor = db.get_collection("movies").aggregate(pipeline)
        # print(len(await cursor.to_list(length=None)))
        # cursor = await db.get_collection("movies").find_one({"title": "Life Is Beautiful"},
        #                                                     {"_id": False, "cast": True, "writers": True})
        # pprint(cursor)
        cursor = db.get_collection("movies").aggregate(pipeline)
        async for i in cursor:
            pprint(i)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())
