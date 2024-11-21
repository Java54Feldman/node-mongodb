import MongoConnection from "./mongo/MongoConnection.mjs";
const DB_NAME = "sample_mflix";
const COLLECTION_MOVIES_NAME = "movies";
const COLLECTION_COMMENTS_NAME = "comments";

const mongoConnection = new MongoConnection(process.env.MONGO_URI, DB_NAME);
const collectionMovies = mongoConnection.getCollection(COLLECTION_MOVIES_NAME);
const collectionComments = mongoConnection.getCollection(
  COLLECTION_COMMENTS_NAME
);

const query1 = collectionComments
  .aggregate([
    {
      $lookup: {
        from: "movies",
        localField: "movie_id",
        foreignField: "_id",
        as: "movies",
      },
    },
    {
      $match: {
        "movies.0": {
          $exists: true,
        },
      },
    },

    { $limit: 5 },

    {
      $replaceRoot: {
        newRoot: {
          $mergeObjects: [
            {
              title: {
                $arrayElemAt: ["$movies.title", 0],
              },
            },
            "$$ROOT",
          ],
        },
      },
    },

    {
      $project: {
        movies: 0,
        movie_id: 0,
      },
    },
  ])
  .toArray();
  
const query2 = collectionMovies
  .aggregate([
    {
      $facet: {
        // Calculate the average imdb.rating for all movies
        averageRating: [
          {
            $group: {
              _id: null,
              avgRating: { $avg: "$imdb.rating" },
            },
          },
        ],
        // Filter and select comedies from 2010
        comedies2010: [
          {
            $match: {
              year: 2010,
              genres: "Comedy",
              "imdb.rating": { $exists: true },
            },
          },
          {
            $project: {
              _id: 0,
              title: 1,
              "imdb.rating": 1,
            },
          },
        ],
      },
    },
    { $unwind: "$averageRating" },
    { $unwind: "$comedies2010" },
    // Filter comedies with rating above average
    {
      $match: {
        $expr: {
          $gt: ["$comedies2010.imdb.rating", "$averageRating.avgRating"],
        },
      },
    },
    {
      $project: {
        _id: 0,
        title: "$comedies2010.title",
      },
    },
  ])
  .toArray();

  Promise.all([query1, query2]).then(
    data => {
      data.forEach((res, index) => console.log("query" + (index + 1) + "\n", res));
      mongoConnection.closeConnection();
    });
