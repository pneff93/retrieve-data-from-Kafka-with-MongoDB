install.packages("mongolite")
library(mongolite)
connection <- mongo(collection = "trucks",
                    db = "TruckData",
                    url = "mongodb://user:password@mongo:27017")

data <- connection$find()
dataAggr <- connection$aggregate('[
    {
        "$match": {
            "timestamp": {
                "$gte": "2021-05-23"
            }
        }
    }
]')
