// mongosink.cpp

#include "../pch.h"
#include "../util/net/message.h"
#include "../db/dbmessage.h"
#include "../util/net/message_server.h"

using namespace std;
using namespace mongo;

// don't default to 27017, don't want anyone accidentally connecting
// thinking it is a database...
const int port = 27050; 

 class MyMessageHandler : public MessageHandler {
    public:
        virtual void connected( AbstractMessagingPort* p ) {
            cout << "connect\n";
        }

        virtual void disconnected( AbstractMessagingPort* p ) {
            cout << "disconnect\n";
        }

        virtual void process( Message& m , AbstractMessagingPort* port , LastError * le) {
            while ( true ) {
                if ( inShutdown() ) {
                    log() << "got request after shutdown()" << endl;
                    break;
                }

                lastError.startRequest( m , le );

                DbResponse dbresponse;
                try {
                    assembleResponse( m, dbresponse, port->remote() );
                }
                catch ( const ClockSkewException & ) {
                    log() << "ClockSkewException - shutting down" << endl;
                    exitCleanly( EXIT_CLOCK_SKEW );
                }

                if ( dbresponse.response ) {
                    port->reply(m, *dbresponse.response, dbresponse.responseTo);
                    if( dbresponse.exhaustNS.size() > 0 ) {
                        MsgData *header = dbresponse.response->header();
                        QueryResult *qr = (QueryResult *) header;
                        long long cursorid = qr->cursorId;
                        if( cursorid ) {
                            verify( dbresponse.exhaustNS.size() && dbresponse.exhaustNS[0] );
                            string ns = dbresponse.exhaustNS; // before reset() free's it...
                            m.reset();
                            BufBuilder b(512);
                            b.appendNum((int) 0 /*size set later in appendData()*/);
                            b.appendNum(header->id);
                            b.appendNum(header->responseTo);
                            b.appendNum((int) dbGetMore);
                            b.appendNum((int) 0);
                            b.appendStr(ns);
                            b.appendNum((int) 0); // ntoreturn
                            b.appendNum(cursorid);
                            m.appendData(b.buf(), b.len());
                            b.decouple();
                            DEV log() << "exhaust=true sending more" << endl;
                            continue; // this goes back to top loop
                        }
                    }
                }
                break;
            }
        }
 };

void go() {
    MessageServer::Options options;
    options.port = port;
    
    MessageServer * server = createServer( options , new MyMessageHandler() );
    
    server->run();
}

int main(int argc, char **argv, char** envp) {
    cout << "mongosink" << endl;
    cout << "\nWARNING"
         << "\nThis utility (mongosink) eats db requests from a client. This tool is for "
         << "\nQA and client performance testing purposes. "
         << "\n" 
         << endl;
    go();
    return 0;
}

//#include "../util/net/message_server_port.cpp"
