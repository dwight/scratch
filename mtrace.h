// @file mtrace

#include "util/bmp.h"

namespace mongo { 

    unsigned font[] = {
        0xf99f999,
        0xE99E99E,
        0x7888887,
        0xE99999E,
        0xF88E88F,
        0xF88E888,
        0x788CC53,
        0x999f999,
        0x9AC8CA9,
        0x888888F,
        0x9BAB999,
        0x9DDBBB9,
        0x6999996,
        0xE99E888,
        0x6999771,
        0xE99CA99,
        0xf88f11f,
        0xf44444f,
        0x999999f,
        0x999aa66,
        0x99aaa99,
        0x99a2399,
        0x999f222,
        0x9112449};

    struct Canvas {
        int x, y;
        Canvas() { x = y = 0; }
        void right() { x++; }
        void up() { y--; }
        void paint() { }
        void print(const char *p) { 
            while( p ) { 
                print(*p++);
            }
        }
        void print(int ch) { 
            unsigned z = font[ch];
            for( int i = 0; i < 7; i++ ) {
                unsigned line = z & 0xf;
                for( int y = 3; y >= 0; y-- ) { 
                    if( z&(1<<y) ) { 
                        paint(); 
                    }
                    right();
                }
                up();
                z = (z >> 4);
            }
        }
    };

}
