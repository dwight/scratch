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
            print(e.message);
        else {
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
        function(x) { return { _id : x }; }
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
        if (x) {
            output(x.millisToRespond + "ms");
            if (x.setName)
                output(x.setName);
            if (!x.ismaster) {
                if (x.secondary) {
                    output("secondary");
                    sayLag(svr);
                }
                else {
                    output("ismaster:");
                    output(x.ismaster);
                }
            }
            else {
                if (x.setName) {
                    output("primary  ");
                    sayLag(svr);
                }
                else {
                    output("ok");
                }
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
    print("Config servers:\There are the config servers for your sharded cluster; for production you should have 3.");
    print();
    print("The number suffixed with 'ms' is the number of milliseconds measured to send a command from this PC");
    print("to the member in question. A high number might indicate an overloaded server (keep in mind any network");
    print("latency (which is valid) to the node in question from this PC though.");
    print();
    print("Shards:");
    print("The optime field indicates how far in the past from this PC's date/time the 'optime' is for the indicated");
    print("replica set member. Normally lag would indicate that a secondary is 'falling behind' and not keeping up");
    print("with its primary. However if there have been no writes to the shard, the number is then simply how long");
    print("it has been since any write has occurred on that shard. Also clocks may vary. Compare the number to the optime");
    print("value for the replica set primary to get an accurate depiction of the lag. The optime relative to 'now' is ");
    print("shown to provide some context in case the primary is offline when cluster.ps() is invoked.");
    print();
}

cluster.help = function () {
    print("\nHelp");
    print();
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
    print("  x.get(n)                     return a DBConnection to the nth server in list");
    print();
    print("Examples:");
    print("  cluster.configs().status()");
    print("  cluster.mongos().get(0).serverStatus()");
    print('  cluster.mongod().get(2)._adminCommand("replSetGetStatus");');
    print('  cluster.primaries().get(0).getSisterDB("test").stats()');
    print();
}

