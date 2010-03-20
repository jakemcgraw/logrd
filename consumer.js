var sys = require("sys"),
fs = require("fs"),
http = require("http"),
redis = require("../redis-node-client/redisclient"),
store = null;

var noop = function(){};

Date.prototype.getRealMonth = function(){
  return this.getMonth()+1;
};

Date.prototype.getNumbers = function(section){
  var func = ['FullYear', 'RealMonth', 'Date', 'Hours', 'Minutes', 'Seconds'],
      offs = {year:10, month:8, day:6, hour:4, min:2, sec:0}[section||"sec"],
      self = this;
  return func.map(function(n){return self['get'+n]().toString().replace(/^(\d)$/, '0$1');}).join("").slice(0, 14-offs);
};

function Consumer(config) {
  var default_config = {
    consumeCount : 250,
    consumeInterval : 1000,
    redisHost : "127.0.0.1",
    redisPort : 6379
  };
  this.config = {};
  for(var i in default_config) {
    this.config[i] = config.hasOwnProperty(i) ?
      config[i] : default_config[i];
  }
  store = new redis.Client(this.config.redisPort, this.config.redisHost);
  this.init();
}

Consumer.prototype.init = function(){
  store.connect(noop);
};

Consumer.prototype.consume = function(){
  var self = this;
  this.loop && clearInterval(this.loop);
  this.loop = setInterval(function(){
    store.smembers("global:producers", function(err, producers){
      if (!producers || !producers.length) return;
      for(var i=0; i<producers.length; i++) {
        (function(pid){
          var messages = [], start_time = (new Date()).getTime(), job_mem;
          store.llen("queue:"+pid+":messages", function(err, length){
            if (!length) return;
            
            sys.puts("found "+length+" messages for "+pid);
            
            store.incr("global:jobs:count", function(err, jid){
              job_mem = JSON.stringify([jid, pid]);
              
              sys.puts("starting job "+job_mem);
              
              store.sadd("global:jobs", job_mem, noop);
              var done = false;
              
              var pop_queue = function(){
                store.rpoplpush("queue:"+pid+":messages", "global:jobs:"+jid, function(err, message){
                  if (message) {
                    messages.push(message);
                    if (messages.length < self.config.consumeCount) {
                      pop_queue();
                      return;
                    }
                  }
                  done = true;
                });
              };
              var chk_queue = function(){
                if (!done) {
                  return process.nextTick(chk_queue);
                }
                self.processMessages(pid, messages, function(){
                  var finish_time = (new Date()).getTime(),
                    total_time_seconds = (finish_time/start_time)/1000;
                  store.del("global:jobs:"+jid, noop);
                  store.srem("global:jobs", job_mem, noop);
                  store.sadd("global:jobs:stats", JSON.stringify([jid, pid, start_time, finish_time, total_time_seconds]));
                  sys.puts("finished job "+job_mem+" took "+total_time_seconds+" sec");
                });
              };
              pop_queue();
              chk_queue();
            });
          });
        })(producers[i]);
      }
    });
  }, this.config.consumeInterval);
};

Consumer.prototype.processDateDate = (function(){
  var months  = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
      pattern = /^(\d{2})\/(\w{3})\/(\d{4}):(\d{2}):(\d{2}):(\d{2}) (-?\d{4})$/;
  return function(date) {
    var m = null;
    if (m = pattern.exec(date)) {
      var month = (months.indexOf(m[2])+1).toString().replace(/^(\d)$/, '0$1');
      return m[3]+month+m[1]+m[4]+m[5]+m[6];
    }
    return "";
  };
})();

Consumer.prototype.processDateTime = (function(){
  var months  = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
      pattern = /^(\d{2})\/(\w{3})\/(\d{4}):(\d{2}):(\d{2}):(\d{2}) (-?\d{4})$/;
  return function(date){
    var m = null;
    if (m = pattern.exec(date)) {
      var ints = [m[3], m[1], m[4], m[5], m[6]].map(function(n){
        return parseInt(n.replace(/^0/, ''));
      });
      return Math.ceil((new Date(ints[0], months.indexOf(m[2]), ints[1], ints[2], ints[3], ints[4], 0)).getTime()/1000);
    }
    return 0;
  };
})();

Consumer.prototype.processIp = (function(){
  var octals = (new Array(4)).map(function(n,i){return Math.pow(256, i)});
  octals.reverse();
  return function(ipaddress){
    return ipaddress.split(".").map(function(n, i){return n*octals[i];}).reduce(function(a,b){return a+b}, 0);
  };
})();

Consumer.prototype.processMessage = (function(){
  var fields = ["client", "remotelog", "remoteuser", "datestr", "request", "status", "size", "referrer", "useragent"];
  return function(raw_message) {
    var self = this,
        args = raw_message.split(/\s+/),
        flds = fields.slice(0),
        retr = {},
        valu = "";
    for(var i=0, len=args.length; i<len; i++) {
      valu = args[i];
      if (valu.match(/^("|\[)/) && !valu.match(/("|\])$/)) {
        while(true) {
          valu += " "+args[++i];
          if (args[i].match(/("|\])$/)) break;
        }
      }
      retr[flds.shift()] = valu.replace(/("|\[|\])/g, "");
    }
    retr.clientid = self.processIp(retr.client);
    retr.time = self.processDateTime(retr.datestr);
    retr.datetime = self.processDateDate(retr.datestr);
    return retr;
  };
})();

Consumer.prototype.processMessages = function(pid, messages, callback) {
  var timestamps  = [],
      datetimes   = [],
      hits        = [],
      msg         = {},
      tsidx       = 0,
      tomap = [["sec", 1], ["min", 60], ["hour", 3600], ["day", 86400]]
      
  while(messages.length) {
    msg = this.processMessage(messages.pop());
    if (msg.time && "" != msg.datetime) {
      if (-1 == (tsidx = timestamps.indexOf(msg.time))) {
        tsidx = timestamps.push(msg.time)-1;
        datetimes[tsidx] = msg.datetime;
      }
      if (hits[tsidx]) {
        hits[tsidx] += 1;
      }
      else {
        hits[tsidx] = 1;
      }
    }
  }
  
  for (var tsidx=0, len=timestamps.length, ts, dt, ht; tsidx<len; tsidx++) {
    ts = timestamps[tsidx],
    dt = datetimes[tsidx],
    ht = hits[tsidx];
    
    // Total global hits
    store.incrby("global:access:hits:all", ht, noop);
    
    // Total producer hits
    store.incrby("producers:"+pid+":access:hits:all", ht, noop);
    
    // Break out hits by time segment (second, minute, hour, day)
    for(var i=0; i<4; i++) {
      (function(key, rank, value, total){
        // Global hits
        store.zadd("global:access:keys:"+key, rank, value, function(){
          store.incrby("global:access:hits:"+key+":"+value, total, noop);
        });
        
        // Producer hits
        store.zadd("producers:"+pid+":access:keys:"+key, rank, value, function(){
          store.incrby("producers:"+pid+":access:hits:"+key+":"+value, total, noop);
        });
        
      })(tomap[i][0], Math.ceil(ts/tomap[i][1]), dt.slice(0, dt.length-(i*2)), ht);
    }
  }
  // Callback when finished
  callback();
};

Consumer.prototype.generateDateRange = function(interval, time, length) {
  var rate = {sec:1, min:60, hour:3600, day:(3600*24)}[interval] * 1000,
      retr = [];
  for(var i=0; i<=length; i++) {
    retr.push((new Date(time+i*rate)).getNumbers(interval));
  }
  return retr;
};

Consumer.prototype.report = function(pid, interval, start, end, callback) {
  var rate = {sec:1, min:60, hour:3600, day:(3600*24)}[interval] * 1000,
      t_st = Math.ceil(start/rate),
      t_en = Math.ceil(end/rate),
      size = t_en-t_st,
      prfx = "producers:"+pid+":access:",
      alld = this.generateDateRange(interval, start, size);
      
  store.zrangebyscore(prfx+"keys:"+interval, t_en-size, t_en, function(err, keys){
    if (keys && keys.length) {
      var args = keys.map(function(n){
        return prfx+"hits:"+interval+":"+n;
      });
      args.push(function(err, hits){
        var retr = {label: pid, data:[]}, j = 0;
        for(var i=0; i<alld.length; i++) {
          if (-1 == (j = keys.indexOf(alld[i]))) {
            retr.data.push([alld[i], 0]);
          }
          else {
            retr.data.push([keys[j], hits[j]||0]);
          }
        }
        callback(retr);
      });
      store.mget.apply(store, args);
    }
    else {
      callback({label:pid, data: alld.map(function(n){return [n, 0];})});
    }
  });
};

Consumer.prototype.reportLatest = function(pid, interval, size, callback) {
  var rate = {sec:1, min:60, hour:3600, day:(3600*24)}[interval] * 1000,
      time = (new Date()).getTime();
  this.report(pid, interval, time-(size*rate), time, function(result){
    result.data = result.data.map(function(n){
      switch(interval) {
        case "min":
          return [n[0].substr(-4), n[1]];
      }
      return n;
    });
    callback(result);
  });
};

var server = new Consumer({});
server.consume();
sys.puts("watching for updates");

http.createServer(function (req, res) {
  if (req.url == "/") {
    sys.puts("application request");
    fs.readFile("/Users/macbook/Projects/logrd/application/index.html", null, function(err, contents){
      res.writeHead(200, {"Content-type": "text/html"});
      res.write(contents, "utf8");
      res.close();
    });
  }
  else if (req.url == "/report.json"){
    sys.puts("generating report");
    server.reportLatest("producer2", "min", 15, function(result){
      res.writeHead(200, {'Content-Type': 'application/json'});
      res.write(JSON.stringify(result));
      res.close();
    });
  }
}).listen(8000);
sys.puts('server running at http://127.0.0.1:8000/');