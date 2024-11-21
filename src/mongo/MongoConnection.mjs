import { MongoClient } from "mongodb";
export default class MongoConnection {
    #db 
    #client
    // # - private
    constructor(connectionStr, dbName) {
        this.#client = new MongoClient(connectionStr);
        this.#db = this.#client.db(dbName);
    }
    getCollection(collectionName) {
        return this.#db.collection(collectionName);
    }
    closeConnection() {
        this.#client.close();
    }
}