var sys = require("sys"),
redis = require("../redis-node-client/redisclient"),
store = null;

String.prototype.trim = function(){
  return this.replace(/(^\s+|\s+$)/g, "");
};

function Producer(config) {
  var default_config = {
    file:"/var/log/apache2/access.log",
    overwrite : true,
    produceCount : 250,
    produceInterval : 50,
    redisHost : "localhost",
    redisPort : 6379
  };
  this.config = {};
  for(var i in default_config) {
    this.config[i] = config.hasOwnProperty(i) ?
      config[i] : default_config[i];
  }
  this.partial = "";
  this.queue = [];
  this.producing = 0;
  store = new redis.Client(this.config.redisPort, this.config.redisHost);
  this.init();
};

/**
 * Initialize producer, let consumers know we're recording
 */
Producer.prototype.init = function(){
  var self = this;
  
  var add_unique_name = function(){
    store.incr("global:producer:counter", function(err, count){
      self.config.name = "producer"+count;
      store.sadd("global:producers", self.config.name);
    });
  };
  store.connect(function(){
    if (!self.config.name) {
      add_unique_name();
    }
    else {
      store.sadd("global:producers", self.config.name, function(err, value){
        if (!value && !self.config.overwrite) {
          sys.puts("WARN - Name "+self.config.name+" already registered");
          add_unique_name();
        }
      })
    }
  });
};

/**
 * Read messages off of internal queue, push into Redis
 */
Producer.prototype.produce = function(){
  var self = this;
  
  this.loop && clearInterval(this.loop);
  // Add producer
  this.loop = setInterval(function(){
    if (self.queue.length) {
      if (!self.producing) {
        sys.print("processing changes...");
      }
      var batch = self.queue.splice(0, self.config.produceCount);
      self.producing += batch.length;
      store.connect(function(){
        for(var i=0, len=batch.length; i<len; i++) {
          store.rpush("queue:"+self.config.name+":messages", batch[i]);
        }
      });
    }
    else if (self.producing) {
      sys.puts("done, sent "+self.producing+" messages");
      self.producing = 0;
    }
  }, this.config.produceInterval);
};

/**
 * Read whole lines off of IO, push onto internal queue
 */
Producer.prototype.consume = function(){
  var self = this;
  
  // Activate producer
  this.produce();
  this.command && this.command.kill();
  this.command = process.createChildProcess("tail", ["-f", this.config.file]);
  
  // Add consumer
  this.command.addListener("output", function(chunk){
    if (chunk) {
      if ("" != self.partial) {
        chunk = self.partial+chunk;
        self.partial = "";
      }
      var data = chunk.split("\n");
      if (!chunk.charAt(chunk.length-1) != "\n") {
        self.partial = data.pop();
      }
      self.queue = self.queue.concat(data.map(function(n){return n.trim();}).filter(function(n){return n!="";}));
    }
  });
};

// Process command line variables
var config = {};
if (process.argv.length) {
  process.argv.forEach(function(n, i, a){
    if (n.charAt(0) == '-') {
      if (n.length == 2){
        switch(n.charAt(1)) {
          case 'f':
            config.file = a[++i];
            break;
          case 'n':
            config.name = a[++i];
            break;
        }
      }
      else if (n.charAt(1) == '-') {
        switch(n.slice(2)) {
          case 'help':
            
        }
      }
    }
  });
}

client = new Producer(config);
client.consume();
sys.puts("watching for changes in "+client.config.file);