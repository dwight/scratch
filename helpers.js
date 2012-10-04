var cluster = {};

function check(msg, t) {
    if (!t)
        throw msg;
}

function attempt(f) {
    try {
        f() 
    }
    catch (e) {
        if (isObject(e) && e.message)
            print("  exception: " + e.message);
        else {
            print("exception:");
            printjson(e);
        }
    }
}

var ServerList = function (L) {
    this.list = L;
}

ServerList.prototype.shellPrint = function () {
    print("ServerList");
    printjson(this.list);
}

cluster._connPool = {}

cluster._conn = function (h) {
    var p = cluster._connPool;
    if (!p[h]) {
        var k = new Mongo(h).getDB('admin');
        p[h] = k;
    }
    return p[h];
}

ServerList.prototype.length = function () {
    return this.list.length;
}

ServerList.prototype.get = function (x) {
    return cluster._conn(this.list[x]._id);
}

// catches exceptions
cluster._runCommand = function (server, command) {
    var o = {};
    attempt(function () {
        var c = cluster._conn(server);
        var now = new Date();
        o = c.runCommand(command);
        var ms = new Date() - now;
        if (o.ok)
            delete o.ok; // densify
        if (o.maxBsonObjectSize == 16777216)
            delete o.maxBsonObjectSize;
        o.millisToRespond = ms;
    });
    return o;
}

cluster._serverStatus = function (server) {
    return cluster._runCommand(server, "isMaster");
}

ServerList.prototype.status = function () {
    var L = this.list;
    var res = {};
    L.forEach(function (thing) {
        if (!thing || !thing._id) {
            print("server: ???");
            return;
        }
        var server = thing._id;
        res[server] = cluster._serverStatus(server);
        if( thing.type == "configsvr" ) {
            res[server].chunks = null;
            attempt( function() { 
                res[server].chunks = cluster._conn(server).getSisterDB("config").chunks.count();
            });
        }
    });
    return res;
}

cluster.primaries = function () {
    var x = cluster.shards().map(
        function (x) {
            var h = x.hosts;
            for (var i in h) {
                var o = null;
                attempt(function () {
                    o = cluster._conn(h[i]._id).isMaster();
                });
                if (o.primary) {
                    return { _id: o.primary };
                    break;
                }
            }
            return null; // no primary for this shard. placeholder.
        });
    return new ServerList(x);
}

cluster.configs = function () {
    var a = db.getSisterDB("admin");
    var o = a.runCommand("netstat");
    check("not connected to a mongos / sharded cluster", o.isdbgrid);
    return new ServerList( 
      o.configserver.split(',').map(
        function(x) { return { _id : x, type:'configsvr' }; }
      )
    );
}

cluster.mongos = function () {
    var c = db.getSisterDB("config");
    var L =
      c.mongos.find().toArray().map(function (x) { delete x.ping; delete x.waiting; return x; });
    return new ServerList(L);
}

cluster.shards = function () {
    var c = db.getSisterDB("config");
    var S;
    try {
        S = c.shards.find().toArray();
    } catch (e) {
        print("cluster.shards() : couldn't query config.shards collection, error: " + e);
        return [];
    }
    return S.map(
        function (x) {
            return { hosts: x.host.split(',').map(function (h) {
                var y = h.split('/'); // "replsetname/hostname"
                return { _id: y[y.length - 1] };
            })
            };
        });
}

cluster.mongod = function () {
    var c = db.getSisterDB("config");
    var L = [];
    var s = -1;
    c.shards.find().toArray().forEach(
        function (x) {
            s++;
            return { hosts: x.host.split(',').map(function (h) {
                var y = h.split('/'); // "replsetname/hostname"
                L.push({ _id: y[y.length - 1], shard:s });
            })
            };
        });
    return new ServerList(L);
}

cluster.all = function () {
    var c = cluster.configs().list.map(function (x) { x.type = "configsvr"; return x });
    var d = cluster.mongod().list.map(function (x) { x.type = "d"; return x });
    var s = cluster.mongos().list.map(function (x) { x.type = "s"; delete x.up; return x });
    return new ServerList(c.concat(d).concat(s));
}

cluster.ps = function () {
    var tm = null;
    var firstServer;
    var twarned = false;
    function checkTime(svr, x) {
        if (!x)
            return;
        if (tm == null) {
            tm = x.localTime;
            firstServer = svr;
            if (Math.abs(x.localTime - new Date()) > 60 * 15 * 1000) {
                print("  Warning system time for " + svr + " (" + x.localTime + ")");
                print("  varies a lot from your local machine's clock (" + Date() + ")");
            }
        }
        else if (!twarned && Math.abs(x.localTime - tm) > 60 * 5 * 1000) {
            twarned = true;
            print("  Warning system time for    " + svr + " (" + x.localTime + ")");
            print("  varies a good bit relative " + firstServer + " (" + tm + ")");
            print("  Consider checking your server date/times and/or running ntp.");
        }
    }
    var s;
    function output(x) {
        s += ' ';
        s += x;
    }
    // return lag to local clock on this machine
    function getLag(hostname) {
        var res = cluster._runCommand(hostname, "replSetGetStatus");
        if (res && res.members) {
            for (var i in res.members) {
                var o = res.members[i];
                if (o.self && o.optimeDate) {
                    //printjson(o.optimeDate);
                    return (new Date() - o.optimeDate) / 1000.0;
                }
            }
        }
    }
    function ago(seconds) {
        var unit = "secs";
        var x = seconds;
        if (x >= 3600) {
            x /= 3600;
            x = Math.floor(x * 10) / 10;
            unit = "hours";
        }
        else if (x >= 120) {
            x /= 60;
            x = Math.floor(x * 10) / 10;
            unit = "min";
        }
        else {
            x = Math.floor(x);
        }
        return "" + x + unit;
    }
    function sayLag(hostname) {
        var L = getLag(hostname);
        if (L >= 1) {
            output("optime:now-" + ago(L));
        }
    }
    function say(svr, x, prefix) {
        s = '  ';
        //if (prefix) s += prefix;
        s += svr;
        if (friendlyEqual(x, {})) {
            output("???");
        }
        else if (x) {
            if (isNumber(x.millisToRespond)) {
                output(x.millisToRespond + "ms");
            } else {
                print("millisToRespond not set, dump:");
                printjson(x);
                output("?");
            }
            if (x.setName) {
                output(x.setName);
            }
            if (!x.ismaster) {
                if (x.secondary) {
                    output("SECONDARY");
                    sayLag(svr);
                }
                else {
                    output("ismaster:");
                    output(x.ismaster);
                }
            }
            else {
                if (x.setName) {
                    output("PRIMARY  ");
                    sayLag(svr);
                }
                else {
                    output("ok");
                }
            }
            if (x.chunks) {
                output(x.chunks);
            }
        }
        print(s);
        checkTime(svr, x);
    }
    print("for more info type cluster.ps.help()");
    print("\nConfig servers");
    var c = cluster.configs().status();
    for (var i in c) {
        say(i, c[i]);
    }
    print();
    print("Shards");
    {
        var n = 0;
        var s = this.shards().forEach(function (x) {
            print(" " + n++);
            x.hosts.forEach(function (y) {
                say(y._id, cluster._serverStatus(y._id), " ");
            });
        });
    }
    print();
    print("mongos");
    c = cluster.mongos().status();
    for (var i in c) {
        say(i, c[i]);
    }
    print();
}

cluster.ps.help = function () {
    print("\ncluster.ps()");
    print();
    print("This helper provides general info on the state and health of a sharded cluster.");
    print();
    print("The number suffixed with 'ms' is the number of milliseconds measured to send a");
    print("command from this PC to the server in question. A high number might indicate");
    print("an overloaded server (keep in mind any network latency (which is valid) to the");
    print("node in question from this PC though.");
    print();
    print("Config servers");
    print("--------------");
    print("Lists some details on the config servers for your sharded cluster.");
    print();
    print("The number after 'ok' is the number of chunks the config server is aware of.");
    print("This number should agree amoung the config servers else something is wrong.");
    print("Note however that this shell script is querying the config servers one by one;");
    print("thus the output here is not a true snapshot so a tiny variation might be ok;");
    print("if you see that, try running this method or cluster.configs().status()");
    print("several times to verify that the number of chunks eventually converges in");
    print("this script's sampling.");
    print();
    print("Shards");
    print("------");
    print("The optime field indicates how far in the past from this PC's date/time the");
    print("'optime' is for the indicated replica set member. Normally lag would indicate");
    print("that a secondary is 'falling behind' and not keeping up with its primary.");
    print("However if there have been no writes to the shard, the number is then simply");
    print("how long it has been since any write has occurred on that shard. Also clocks");
    print("may vary. Compare the number to the optime value for the replica set primary");
    print("to get an accurate depiction of the lag. The optime relative to 'now' is ");
    print("shown to provide some context in case the primary is offline when cluster.ps()");
    print("is invoked.");
    print();
    print("mongos");
    print("------");
    print("This is a list of currently active mongos processes for the cluster, with their");
    print("status.");
    print();
}

ServerList.prototype.help = function () {
    print();
    print("Certain methods under var 'cluster' (see cluster.help()) return a");
    print("ServerList object.  For example cluster.mongos().");
    print();
    print("ServerList methods:");
    print("  ServerList.length()          number of servers in the 'list'");
    print("  ServerList.get(i)            get a connection to server i.");
    print("  ServerList.status()          return status of all servers in the list");
    print();
    print("Examples:");
    print("  cluster.configs().status()");
    print("  cluster.primaries().get(0).runCommand(\"serverStatus\")");
    print();
}

cluster.help = function () {
    print("connect to a mongos to use these helpers");
    print();
    print("  cluster.ps()                 print some summary info on the cluster");
    print("  cluster.shards()             detail each shard");
    print();
    print("These return a 'ServerList' object:");
    print("  cluster.configs()            returns list of config servers for cluster");
    print("  cluster.mongos()             current mongos servers for cluster");
    print("  cluster.primaries()");
    print("  cluster.mongod()");
    print("  cluster.all()");
    print();
    print("ServerList methods:");
    print("  x.status()");
    print("  x.list");
    print("  x.length()");
    print("  x.get(n)                     return a DBConnection to the nth server in list");
    print();
    print("Examples:");
    print("  cluster.configs().status()");
    print("  cluster.mongos().get(0).serverStatus()");
    print('  cluster.mongod().get(2)._adminCommand("replSetGetStatus");');
    print('  cluster.primaries().get(0).getSisterDB("test").stats()');
    print();
}

