const redis = require('redis');

const CHECK_GENERATOR_COUNT = 10;
const NEW_MESSAGE_RECEIVE_DELAY = 200; // in ms;
const NEW_TRY_DELAY = 1000; // in ms;
const NEW_MESSAGE_GENERATE_DELAY = 500; // in ms;
const LAST_GENERATOR_TIME = 'Last_time_generator_was_alive';
const ERRORS_QUEUE_TAG = '-Errors';
const _DEV_ = true;

class MessageQueue {
    constructor (queueName, isGenerator = false) {
        this._isGenerator = isGenerator;
        this._queueName = queueName;
        this._count = 0;
        this._stop = true;
    }

    run() {
        this._client = redis.createClient();
        this._client.on('error', function(err) {
             this._log('Error occurred ', err)
        });
        this._stop = false;
        if (this._isGenerator) {
            this.generateMessage();
        } else {
            this.checkGeneratorIsAlive();
        }
    }

    stop() {
        this._stop = true;
    }

    getErrors() {
        this._client.lpop(this._queueName + ERRORS_QUEUE_TAG, (err, reply) => {
            if (err) throw err;
            if (reply === null) {
                this._log('All errors processed');
                this._closeConnection();
            } else {
                this._log(reply);
                setTimeout(this.getErrors.bind(this), 0);
            }
        });
    }

    getMessages() {
        if (this._stop) return this._closeConnection();

        if (this._count === CHECK_GENERATOR_COUNT) {
            this._count -= CHECK_GENERATOR_COUNT;
            return this.checkGeneratorIsAlive();
        }

        this._count++;
        this._client.lpop(this._queueName, (err, reply) => {
            if (err) throw err;
            if (reply === null) {
                setTimeout(this.getMessages.bind(this), NEW_TRY_DELAY);
            } else {
                if (parseInt(Math.random() * 100) % 20 == 0) {
                    this._log('Error occurred in message: ', reply);
                    this._client.rpush(this._queueName + ERRORS_QUEUE_TAG, reply);
                } else {
                    redis.print(err, reply);
                }
                setTimeout(this.getMessages.bind(this), NEW_MESSAGE_RECEIVE_DELAY);
            }
        });
    }

    checkGeneratorIsAlive() {
        this._log('Checking generator is alive...');
        this._client.exists(LAST_GENERATOR_TIME, (err, reply) => {
            if (reply === 0) {
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
        if (this._stop) return this._closeConnection();
        const genString = this._queueName +  parseInt(Math.random() * 10) + Date.now();
        const multiQuery = this._client.multi();
        multiQuery.set(LAST_GENERATOR_TIME, 1);
        multiQuery.expire(LAST_GENERATOR_TIME, 10);
        multiQuery.rpush(this._queueName, genString);
        multiQuery.exec((err, reply) => {
            if (err) throw err;
        });
        this._log('Message generated: ', genString);
        setTimeout(this.generateMessage.bind(this), NEW_MESSAGE_GENERATE_DELAY);
    }

    _log(...arr) {
        if (_DEV_) {
            console.log(...arr);
        }
    }

    _closeConnection() {
        this._client.quit();
    }

}

module.exports = MessageQueue;
