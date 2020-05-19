
#define DEBUG 1
#define USE_STDOUT 1
#include "io/io.h"
#include "dal/dal.h"
#include "io/testing/bufferfuncs.c"

#include <unistd.h>
#include <stdio.h>


int main( int argc, char** argv ) {
   xmlDoc *doc = NULL;
   xmlNode *root_element = NULL;


   /*
   * this initialize the library and check potential ABI mismatches
   * between the version it was compiled for and the actual shared
   * library used.
   */
   LIBXML_TEST_VERSION

   /*parse the file and get the DOM */
   doc = xmlReadFile("./testing/config.xml", NULL, XML_PARSE_NOBLANKS);

   if (doc == NULL) {
     printf("error: could not parse file %s\n", "./dal/testing/config.xml");
     return -1;
   }

   /*Get the root element node */
   root_element = xmlDocGetRootElement(doc);

   // Initialize a posix dal instance
   DAL_location maxloc = { .pod = 1, .block = 1, .cap = 1, .scatter = 1 };
   DAL dal = init_dal( root_element, maxloc );

   /* Free the xml Doc */
   xmlFreeDoc(doc);
   /*
   *Free the global variables that may
   *have been allocated by the parser.
   */
   xmlCleanupParser();

   // check that initialization succeeded
   if ( dal == NULL ) {
      printf( "error: failed to initialize DAL: %s\n", strerror(errno) );
      return -1;
   }


   // Test WRITE thread logic
   
   // create a global state struct
   gthread_state gstate;
   gstate.objID = "";
   gstate.location = maxloc;
   gstate.dmode = DAL_WRITE;
   gstate.dal = dal;
   gstate.offset = 0;
   gstate.minfo.N = 1;
   gstate.minfo.E = 0;
   gstate.minfo.O = 0;
   gstate.minfo.partsz = 4096;
   gstate.minfo.versz = 1048576;
   gstate.minfo.blocksz = 0;
   gstate.minfo.crcsum = 0;
   gstate.minfo.totsz = 0;
   gstate.meta_error = 0;
   gstate.data_error = 0;

   // create an ioqueue for our data blocks
   gstate.ioq = create_ioqueue( gstate.minfo.versz, gstate.minfo.partsz, gstate.dmode );
   if ( gstate.ioq == NULL ) {
      LOG( LOG_ERR, "Failed to create IOQueue for write thread!\n" );
      return -1;
   }

   // create a thread state reference
   void* tstate;

   // run our write_init function
   if ( write_init( 0, (void*)&gstate, &tstate ) ) {
      LOG( LOG_ERR, "write_init() function failed!\n" );
      return -1;
   }

   // loop through writing out our test data
   int prtcnt = 0;
   int prtlimit = 10;
   ioblock* iob = NULL;
   ioblock* pblock = NULL;
   while ( prtcnt < prtlimit ) {
      int resres = 0;
      while ( (resres = reserve_ioblock( &iob, &pblock, gstate.ioq )) == 0 ) {
         // get a target for our buffer
         void* fill_tgt = ioblock_write_target( iob );
         if ( fill_tgt == NULL ) {
            LOG( LOG_ERR, "Failed to get fill target for ioblock!\n" );
            return -1;
         }

         // fill our ioblock
         size_t fill_sz = fill_buffer( partcnt * gstate.minfo.partsz, gstate.minfo.iosz, gstate.minfo.partsz, fill_tgt, gstate.dmode );
         if ( fill_sz != gstate.minfo.partsz ) {
            LOG( LOG_ERR, "Expected to fill %zu bytes, but instead filled %zu\n", gstate.minfo.partsz, fillsz );
            return -1;
         }
         ioblock_update_fill( iob, fill_sz );
      }
      // check for an error condition forcing loop exit
      if ( resres < 0 ) {
         LOG( LOG_ERR, "An error occured while trying to reserve a new IOBlock for writing\n" );
         return -1;
      }

      // otherwise, pblock should now be populated, run our write_consume function
      if ( write_consume( &tstate, (void**) &pblock ) ) {
         LOG( LOG_ERR, "write_consume() function indicates an error!\n" );
         return -1;
      }
      pblock = NULL;
      prtcnt++;
   }

   // set some of our minfo values
   gstate.minfo.totsz = ( prtcnt * gstate.minfo.partsz );

   // finally, call our write thread termination function
   write_term( &tstate, (void**) &pblock );

   // check for any write errors
   if( gstate.meta_error ) {
      LOG( LOG_ERR, "Write thread global state indicates a meta error was encountered!\n" );
   }
   if ( gstate.data_error ) {
      LOG( LOG_ERR, "Write thread global state indicates a data error was encountered!\n" );
   }

   // fix our state values
   gstate.dmode = DAL_READ;
   tstate = NULL;

   // call our read_init function
   if ( read_init( 0, (void**) &gstate, &tstate ) ) {
      


   // Delete the block we created
   //if ( dal->del( dal->ctxt, maxloc, "" ) ) { printf( "warning: del failed!\n" ); }

   // Free the DAL
   if ( dal->cleanup( dal ) ) { printf( "error: failed to cleanup DAL\n" ); return -1; }

   // Finally, compare our structs
//   int retval=0;
//   if ( minfo_ref.N != minfo_fill.N ) {
//      printf( "error: set (%d) and retrieved (%d) meta info 'N' values do not match!\n", minfo_ref.N, minfo_fill.N );
//      retval=-1;
//   }
//   if ( minfo_ref.E != minfo_fill.E ) {
//      printf( "error: set (%d) and retrieved (%d) meta info 'E' values do not match!\n", minfo_ref.E, minfo_fill.E );
//      retval=-1;
//   }
//   if ( minfo_ref.O != minfo_fill.O ) {
//      printf( "error: set (%d) and retrieved (%d) meta info 'O' values do not match!\n", minfo_ref.O, minfo_fill.O );
//      retval=-1;
//   }
//   if ( minfo_ref.partsz != minfo_fill.partsz ) {
//      printf( "error: set (%zd) and retrieved (%zd) meta info 'partsz' values do not match!\n", minfo_ref.partsz, minfo_fill.partsz );
//      retval=-1;
//   }
//   if ( minfo_ref.versz != minfo_fill.versz ) {
//      printf( "error: set (%zd) and retrieved (%zd) meta info 'versz' values do not match!\n", minfo_ref.versz, minfo_fill.versz );
//      retval=-1;
//   }
//   if ( minfo_ref.blocksz != minfo_fill.blocksz ) {
//      printf( "error: set (%zd) and retrieved (%zd) meta info 'blocksz' values do not match!\n", minfo_ref.blocksz, minfo_fill.blocksz );
//      retval=-1;
//   }
//   if ( minfo_ref.crcsum != minfo_fill.crcsum ) {
//      printf( "error: set (%lld) and retrieved (%lld) meta info 'crcsum' values do not match!\n", minfo_ref.crcsum, minfo_fill.crcsum );
//      retval=-1;
//   }
//   if ( minfo_ref.totsz != minfo_fill.totsz ) {
//      printf( "error: set (%zd) and retrieved (%zd) meta info 'totsz' values do not match!\n", minfo_ref.totsz, minfo_fill.totsz );
//      retval=-1;
//   }

   return 0;
}


