import MongoConnection from "./mongo/MongoConnection.mjs";
const DB_NAME = "sample_mflix";
const COLLECTION_MOVIES_NAME = "movies";
const COLLECTION_COMMENTS_NAME = "comments";

const mongoConnection = new MongoConnection(process.env.MONGO_URI, DB_NAME);
const collectionMovies = mongoConnection.getCollection(COLLECTION_MOVIES_NAME);
const collectionComments = mongoConnection.getCollection(COLLECTION_COMMENTS_NAME);

collectionComments
  .aggregate([
    // Limit the selection to the first five documents
    { $limit: 5 },

    // Join data from the movies collection
    {
      $lookup: {
        from: "movies",
        localField: "movie_id",
        foreignField: "_id",
        as: "movie",
      },
    },

    // Unwind the movie array, as $lookup returns an array
    { $unwind: "$movie" },

    // Replace the root document, merging fields from comments and movies
    {
      $replaceRoot: {
        newRoot: {
          $mergeObjects: ["$$ROOT", { title: "$movie.title" }],
        },
      },
    },

    // Project the required fields
    {
      $project: {
        _id: 1,
        name: 1,
        email: 1,
        text: 1,
        date: 1,
        title: 1,
      },
    },
  ])
  .toArray()
  .then((data) => console.log(data))
  .catch((error) => console.error(error));

collectionMovies
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
    // Unwind the results
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
    // Project only the movie title
    {
      $project: {
        _id: 0,
        title: "$comedies2010.title",
      },
    },
  ])
  .toArray()
  .then((results) => {
    console.log("Comedies from 2010 with above-average rating:");
    results.forEach((movie) => console.log(movie.title));
  })
  .catch((error) => console.error("An error occurred:", error));
