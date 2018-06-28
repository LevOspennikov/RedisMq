const MessageQueue = require('./src/MessageQueue');
mq = new MessageQueue('messages', false);
mq.run(); 