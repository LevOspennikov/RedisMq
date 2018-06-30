const redis = require('redis');
const bluebird = require('bluebird');
bluebird.promisifyAll(redis);

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
        if (!this._stop) return;

        this._client = redis.createClient();
        this._client.on('error', function(err) {
             this._log('Error occurred ', err)
        });

        this._stop = false;
        if (this._isGenerator) {
            this._generateMessage();
        } else {
            this._checkGeneratorIsAlive();
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

    _getMessages() {
        if (this._stop) return this._closeConnection();

        if (this._count === CHECK_GENERATOR_COUNT) {
            this._count -= CHECK_GENERATOR_COUNT;
            return this._checkGeneratorIsAlive();
        }

        this._count++;
        this._client.lpopAsync(this._queueName).then((reply) => {
            if (reply === null) {
                setTimeout(this._getMessages.bind(this), NEW_TRY_DELAY);
            } else {
                try {
                    this._processMessage(reply);
                } catch (err) {
                    this._client.rpush(this._queueName + ERRORS_QUEUE_TAG, reply);
                }
                setTimeout(this._getMessages.bind(this), NEW_MESSAGE_RECEIVE_DELAY);
            }
        }).catch((err) => { throw err });
    }

    _processMessage(message) {
        if (parseInt(Math.random() * 100) % 20 == 0) {
            this._log('Error occurred in message: ', message);
            throw new Error(message);
        } else {
            this._log('Reply: ', message);
        }
    }

    _checkGeneratorIsAlive() {
        this._log('Checking generator is alive...');
        this._client.existsAsync(LAST_GENERATOR_TIME).then((reply) => {
            if (reply === 0) {
                this._client.watch(LAST_GENERATOR_TIME);
                const multiQuery = this._client.multi()
                    .set(LAST_GENERATOR_TIME, 1)
                    .expire(LAST_GENERATOR_TIME, 10);
                return multiQuery.execAsync()
            } else {
                Promise.resolve(null);
            }
        }).then((reply) => {
            if (reply == null) {
                this._getMessages();
            } else {
                this._isGenerator = true;
                this._generateMessage();
            }
        }).catch((err) => { throw err });
    }

    _generateMessage() {
        if (this._stop) return this._closeConnection();

        const genString = this._createMessage();
        const multiQuery = this._client.multi();
        multiQuery.set(LAST_GENERATOR_TIME, 1)
            .expire(LAST_GENERATOR_TIME, 10)
            .rpush(this._queueName, genString);

        multiQuery.execAsync().then(() => {
            this._log('Message generated: ', genString);
            setTimeout(this._generateMessage.bind(this), NEW_MESSAGE_GENERATE_DELAY);
        }).catch(this._onError);
    }

    _createMessage() {
        return this._queueName +  parseInt(Math.random() * 10) + Date.now();
    }

    _log(...arr) {
        if (_DEV_) {
            console.log(...arr);
        }
    }

    _onError(err) {
        throw err;
    }

    _closeConnection() {
        this._client.quit();
    }
}

module.exports = MessageQueue;
