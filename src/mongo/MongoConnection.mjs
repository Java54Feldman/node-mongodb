import { MongoClient } from "mongodb";
export default class MongoConnection {
    #db 
    // # - private
    constructor(connectionStr, dbName) {
        const client = new MongoClient(connectionStr);
        this.#db = client.db(dbName);
    }
    getCollection(collectionName) {
        return this.#db.collection(collectionName);
    }
}