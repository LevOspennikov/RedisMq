'use strict'

const MessageQueue = require('./src/MessageQueue');
const program = require('commander');

program.description("Redis message queue")
  .option('-e, --getErrors', 'Collect all errors')
  .parse(process.argv);

const mq = new MessageQueue('messages', false);
if (!program.getErrors) {
    mq.run();
} else {
    mq.run(true);
}
