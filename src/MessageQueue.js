const redis = require('redis');

const CHECK_GENERATOR_COUNT = 10;
const NEW_MESSAGE_RECEIVE_DELAY = 200; // in ms;
const NEW_TRY_DELAY = 1000; // in ms;
const NEW_MESSAGE_GENERATE_DELAY = 500; // in ms;
const LAST_GENERATOR_TIME = 'Last_time_generator_was_alive';
const ERRORS_QUEUE_TAG = '-Errors';

class MessageQueue {
    constructor (queueName, isGenerator = false) {
        this._isGenerator = isGenerator;
        this._client = redis.createClient();
        this._client.on('error', function(err) {
             console.log('Something went wrong ', err)
        });
        this._queueName = queueName;
        this._count = 0; 
    }

    run() {
        if (this._isGenerator) { 
            this.generateMessage();
        } else {
            this.checkGeneratorIsAlive();
        }
    }

    getMessages() {
        if (this._count == CHECK_GENERATOR_COUNT) {
            this._count -= CHECK_GENERATOR_COUNT;
            return this.checkGeneratorIsAlive();
        } 

        this._count++;
        this._client.lpop(this._queueName, function (err, reply) {
            if (err) throw err;         
            if (reply == null) {
                setTimeout(this.getMessages.bind(this), NEW_TRY_DELAY);
            } else {
                if (parseInt(Math.random() * 100) % 20 == 0) { 
                    console.log('Error occurred in message: ', reply);
                    this._client.rpush(this._queueName + ERRORS_QUEUE_TAG, reply);
                } else { 
                    redis.print(err, reply);
                }
                setTimeout(this.getMessages.bind(this), NEW_MESSAGE_RECEIVE_DELAY);
            }
        }.bind(this));
    }

    getErrors() {
        this._client.lpop(this._queueName + ERRORS_QUEUE_TAG, function (err, reply) {
            if (err) throw err; 
            if (reply == null) {
                console.log('All errors processed');
                this._client.quit();
            } else {
                console.log(reply);
                setTimeout(this.getErrors.bind(this), 0);
            }
        }.bind(this));
    }

    checkGeneratorIsAlive() {
        console.log('Checking generator is alive...');
        this._client.exists(LAST_GENERATOR_TIME, (err, reply) => {
            if (!reply) {
                this._client.watch(LAST_GENERATOR_TIME);
                const multiQuery = this._client.multi();
                multiQuery.set(LAST_GENERATOR_TIME, 1);
                multiQuery.expire(LAST_GENERATOR_TIME, 10);
                multiQuery.exec((err, reply) => {
                    if (err) throw err;
                    if (reply == null) { 
                        this.getMessages();
                    } else {
                        this._isGenerator = true; 
                        this.generateMessage();
                    }
                })
            } else {
                this.getMessages();
            }
        })
    }

    generateMessage() { 
        const genString = this._queueName +  parseInt(Math.random() * 10) + Date.now();
        const multiQuery = this._client.multi();
        multiQuery.set(LAST_GENERATOR_TIME, 1);
        multiQuery.expire(LAST_GENERATOR_TIME, 10);
        multiQuery.rpush(this._queueName, genString);
        multiQuery.exec((err, reply) => {
            if (err) throw err; 
        });
        console.log('Message generated: ', genString);
        setTimeout(this.generateMessage.bind(this), NEW_MESSAGE_GENERATE_DELAY);
    }
}

module.exports = MessageQueue;