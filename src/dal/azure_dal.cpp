//#include "erasureUtils_auto_config.h"
#if defined(DEBUG_ALL) || defined(DEBUG_DAL)
#define DEBUG 1
#endif
#define LOG_PREFIX "azure_dal"
#include "../logging/logging.h"

#include <was/storage_account.h>
#include <was/blob.h>
#include <cpprest/rawptrstream.h>
#include <cpprest/containerstream.h>
#include <stdio.h>
#include <memory>

#include "dal.h"
//#include "azure_dal.h"

//   -------------    AZURE DEFINITIONS    -------------

#define IO_SIZE (5 << 20)    // Preferred I/O Size: 5M
#define NO_OBJID "noneGiven" // Substitute ID when none is provided

//   -------------    AZURE CONTEXT    -------------

azure::storage::cloud_blob_client client;

typedef struct azure_block_context_struct
{
  DAL_MODE mode;
  int next_id;
  azure::storage::cloud_block_blob blob;
  std::vector<azure::storage::block_list_item> block_list;
  char *m_buf;
} * AZURE_BLOCK_CTXT;

typedef struct azure_dal_context_struct
{
  DAL_location max_loc; // Maximum pod/cap/block/scatter values
  char *connectionString;
  azure::storage::cloud_blob_client client;
} * AZURE_DAL_CTXT;

//   -------------    AZURE INTERNAL FUNCTIONS    -------------

/** (INTERNAL HELPER FUNCTION)
 * Calculate the number of decimal digits required to represent a given value
 * @param int value : Integer value to be represented in decimal
 * @return int : Number of decimal digits required, or -1 on a bounds error
 */
static int num_digits(int value)
{
  if (value < 0)
  {
    return -1;
  } // negative values not permitted
  if (value < 10)
  {
    return 1;
  }
  if (value < 100)
  {
    return 2;
  }
  if (value < 1000)
  {
    return 3;
  }
  if (value < 10000)
  {
    return 4;
  }
  if (value < 100000)
  {
    return 5;
  }
  // only support values up to 5 digits long
  return -1;
}

//   -------------    S3 IMPLEMENTATION    -------------

int azure_verify(DAL_CTXT ctxt, char fix)
{
  // No verification needed since containers are created by azure_open;
  return 0;
}

/** NOTE: "online" migrations are not true migrations, they just copy an object from one location
 * to another, with no guarantee that both objects will remain synchronized after the migrate
 * is completed. This is due to Azure Blob Storage's inability for two keys to map to one value.
*/
int azure_migrate(DAL_CTXT ctxt, const char *objID, DAL_location src, DAL_location dest, char offline)
{
  // fail if only the block is different
  if (src.pod == dest.pod && src.cap == dest.cap && src.scatter == dest.scatter)
  {
    LOG(LOG_ERR, "received identical locations!\n");
    return -1;
  }

  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL dal context!\n");
    return -1;
  }
  AZURE_DAL_CTXT dctxt = (AZURE_DAL_CTXT)ctxt; // should have been passed a s3 context

  if (strlen(objID) == 0)
  {
    objID = NO_OBJID;
  }

  try
  {
    azure::storage::cloud_blob_container srcCont = dctxt->client.get_container_reference(U(std::to_string(src.block) + "-" + std::to_string(src.cap) + "-" + std::to_string(src.scatter)));
    azure::storage::cloud_blob_container destCont = dctxt->client.get_container_reference(U(std::to_string(dest.block) + "-" + std::to_string(dest.cap) + "-" + std::to_string(dest.scatter)));
    azure::storage::cloud_block_blob srcBlob = srcCont.get_block_blob_reference(U(objID));
    azure::storage::cloud_block_blob destBlob = destCont.get_block_blob_reference(U(objID));

    destCont.create_if_not_exists();

    destBlob.start_copy(srcBlob);

    if (offline)
    {
      srcBlob.delete_blob();
    }
  }
  catch (const std::exception &e)
  {
    fprintf(stderr, "Error (azure_migrate): %s\n", e.what());
    fflush(stderr);
    return -1;
  }

  return 0;
}

int azure_del(DAL_CTXT ctxt, DAL_location location, const char *objID)
{
  if (ctxt == NULL)
  {
    fprintf(stderr, "received a NULL dal context!\n");
    fflush(stderr);
    return -1;
  }
  AZURE_DAL_CTXT dctxt = (AZURE_DAL_CTXT)ctxt; // should have been passed a azure context

  if (strlen(objID) == 0)
  {
    objID = NO_OBJID;
  }

  try
  {
    // Retrieve a reference to a container.
    azure::storage::cloud_blob_container container = dctxt->client.get_container_reference(U(std::to_string(location.block) + "-" + std::to_string(location.cap) + "-" + std::to_string(location.scatter)));
    azure::storage::cloud_block_blob blob = container.get_block_blob_reference(U(objID));
    blob.delete_blob();
  }
  catch (const std::exception &e)
  {
    fprintf(stderr, "Error (azure_stat): %s\n", e.what());
    fflush(stderr);
    return -1;
  }

  return 0;
}

int azure_stat(DAL_CTXT ctxt, DAL_location location, const char *objID)
{
  if (ctxt == NULL)
  {
    fprintf(stderr, "received a NULL dal context!\n");
    fflush(stderr);
    return -1;
  }
  AZURE_DAL_CTXT dctxt = (AZURE_DAL_CTXT)ctxt; // should have been passed a azure context

  if (strlen(objID) == 0)
  {
    objID = NO_OBJID;
  }

  azure::storage::cloud_blob_container container;
  azure::storage::cloud_block_blob blob;
  try
  {
    // Retrieve a reference to a container.
    container = dctxt->client.get_container_reference(U(std::to_string(location.block) + "-" + std::to_string(location.cap) + "-" + std::to_string(location.scatter)));
    blob = container.get_block_blob_reference(U(objID));
    if (!blob.exists())
    {
      return -1;
    }
  }
  catch (const std::exception &e)
  {
    fprintf(stderr, "Error (azure_stat): %s\n", e.what());
    fflush(stderr);
    return -1;
  }

  return 0;
}

int azure_cleanup(struct DAL_struct *dal)
{
  if (dal == NULL)
  {
    fprintf(stderr, "received a NULL dal!\n");
    fflush(stderr);
    return -1;
  }
  AZURE_DAL_CTXT dctxt = (AZURE_DAL_CTXT)dal->ctxt; // should have been passed an azure context

  // free the DAL struct and its associated state
  delete dctxt;
  delete dal;
  return 0;
}

BLOCK_CTXT azure_open(DAL_CTXT ctxt, DAL_MODE mode, DAL_location location, const char *objID)
{
  if (ctxt == NULL)
  {
    fprintf(stderr, "received a NULL dal context!\n");
    fflush(stderr);
    return NULL;
  }
  AZURE_DAL_CTXT dctxt = (AZURE_DAL_CTXT)ctxt; // should have been passed a azure context

  // allocate space for a new BLOCK context
  AZURE_BLOCK_CTXT bctxt = new azure_block_context_struct;

  if (strlen(objID) == 0)
  {
    objID = NO_OBJID;
  }

  if (bctxt == NULL)
  {
    return NULL;
  } // malloc will set errno

  azure::storage::cloud_blob_container container;

  try
  {
    // Retrieve a reference to a container.
    container = dctxt->client.get_container_reference(U(std::to_string(location.block) + "-" + std::to_string(location.cap) + "-" + std::to_string(location.scatter)));

    // Create the container if it doesn't already exist.
    container.create_if_not_exists();
  }
  catch (const std::exception &e)
  {
    fprintf(stderr, "Error (azure_open 1): %s\n", e.what());
    fflush(stderr);
    delete bctxt;
    return NULL;
  }

  bctxt->mode = mode;
  bctxt->next_id = 0;
  bctxt->block_list.clear();

  if (mode == DAL_READ)
  {
    LOG(LOG_INFO, "Open for READ\n");
  }
  else if (mode == DAL_METAREAD)
  {
    LOG(LOG_INFO, "Open for META_READ\n");
  }
  else // DAL_WRITE or DAL_REBUILD (no difference in this implementation)
  {
    if (mode == DAL_WRITE)
    {
      LOG(LOG_INFO, "Open for WRITE\n");
    }
    else if (mode == DAL_REBUILD)
    {
      LOG(LOG_INFO, "Open for REBUILD\n");
    }
  }

  try
  {
    bctxt->blob = container.get_block_blob_reference(U(objID));
  }
  catch (const std::exception &e)
  {
    fprintf(stderr, "Error (azure_open 2): %s\n", e.what());
    fflush(stderr);
    delete bctxt;
    return NULL;
  }

  return bctxt;
}

int azure_set_meta(BLOCK_CTXT ctxt, const char *meta_buf, size_t size)
{
  if (ctxt == NULL)
  {
    fprintf(stderr, "received a NULL block context!\n");
    fflush(stderr);
    return -1;
  }
  AZURE_BLOCK_CTXT bctxt = (AZURE_BLOCK_CTXT)ctxt; // should have been passed an azure context

  // abort, unless we're writing or rebuliding
  if (bctxt->mode != DAL_WRITE && bctxt->mode != DAL_REBUILD)
  {
    fprintf(stderr, "Can only perform set_meta ops on a DAL_WRITE or DAL_REBUILD block handle!\n");
    fflush(stderr);
    return -1;
  }

  bctxt->m_buf = strdup(meta_buf);
  return 0;
}

ssize_t azure_get_meta(BLOCK_CTXT ctxt, char *meta_buf, size_t size)
{

  if (ctxt == NULL)
  {
    fprintf(stderr, "received a NULL block context!\n");
    fflush(stderr);
    return -1;
  }
  AZURE_BLOCK_CTXT bctxt = (AZURE_BLOCK_CTXT)ctxt; // should have been passed an azure context

  try
  {
    bctxt->blob.download_attributes();
    azure::storage::cloud_metadata &meta = bctxt->blob.metadata();
    strcpy(meta_buf, meta[U("_libne")].c_str());
  }
  catch (const std::exception &e)
  {
    fprintf(stderr, "Error (azure_get_meta): %s\n", e.what());
    fflush(stderr);
    return -1;
  }

  return strlen(meta_buf);
}

int azure_put(BLOCK_CTXT ctxt, const void *buf, size_t size)
{
  if (ctxt == NULL)
  {
    fprintf(stderr, "received a NULL block context!\n");
    fflush(stderr);
    return -1;
  }
  AZURE_BLOCK_CTXT bctxt = (AZURE_BLOCK_CTXT)ctxt; // should have been passed an azure context

  // abort, unless we're writing or rebuliding
  if (bctxt->mode != DAL_WRITE && bctxt->mode != DAL_REBUILD)
  {
    fprintf(stderr, "Can only perform put ops on a DAL_WRITE or DAL_REBUILD block handle!\n");
    fflush(stderr);
    return -1;
  }

  try
  {
    concurrency::streams::rawptr_buffer<uint8_t> r_buf(static_cast<const unsigned char *>(buf), size);
    concurrency::streams::istream i_stream(r_buf);
    utility::string_t b_id = utility::conversions::to_base64(bctxt->next_id++);
    bctxt->block_list.push_back(azure::storage::block_list_item(b_id));
    bctxt->blob.upload_block(b_id, i_stream, utility::string_t());
    i_stream.close().wait();
    r_buf.close().wait();
  }
  catch (const std::exception &e)
  {
    fprintf(stderr, "Error (azure_put): %s\n", e.what());
    fflush(stderr);
    return -1;
  }
  return 0;
}

ssize_t azure_get(BLOCK_CTXT ctxt, void *buf, size_t size, off_t offset)
{
  if (ctxt == NULL)
  {
    fprintf(stderr, "received a NULL block context!\n");
    fflush(stderr);
    return -1;
  }
  AZURE_BLOCK_CTXT bctxt = (AZURE_BLOCK_CTXT)ctxt; // should have been passed an azure context

  try
  {
    concurrency::streams::rawptr_buffer<unsigned char> r_buf((unsigned char *)buf, size);
    concurrency::streams::ostream o_stream = r_buf.create_ostream();
    bctxt->blob.download_range_to_stream(o_stream, offset, size);
    o_stream.close().wait();
    r_buf.close().wait();
  }
  catch (const std::exception &e)
  {
    fprintf(stderr, "Error (azure_get): %s\n", e.what());
    fflush(stderr);
    return -1;
  }
  return size;
}

int azure_abort(BLOCK_CTXT ctxt)
{
  if (ctxt == NULL)
  {
    fprintf(stderr, "received a NULL block context!\n");
    fflush(stderr);
    return -1;
  }
  AZURE_BLOCK_CTXT bctxt = (AZURE_BLOCK_CTXT)ctxt; // should have been passed an azure block context

  // Do not commit any data. Uncommitted blocks should be garbage collected eventually...?

  delete bctxt;
  return 0;
}

int azure_close(BLOCK_CTXT ctxt)
{
  if (ctxt == NULL)
  {
    fprintf(stderr, "received a NULL block context!\n");
    fflush(stderr);
    return -1;
  }
  AZURE_BLOCK_CTXT bctxt = (AZURE_BLOCK_CTXT)ctxt; // should have been passed an azure block context

  // Commit any data written
  if (bctxt->mode == DAL_WRITE || bctxt->mode == DAL_REBUILD)
  {
    if (!bctxt->block_list.empty())
    {
      try
      {
        bctxt->blob.upload_block_list(bctxt->block_list);
      }
      catch (const std::exception &e)
      {
        fprintf(stderr, "Error (azure_close 1): %s\n", e.what());
        fflush(stderr);
        return -1;
      }
    }

    if (bctxt->m_buf)
    {
      try
      {
        bctxt->blob.download_attributes();
        azure::storage::cloud_metadata &meta = bctxt->blob.metadata();
        meta[U("_libne")] = U(bctxt->m_buf);
        bctxt->blob.upload_metadata();
      }
      catch (const std::exception &e)
      {
        fprintf(stderr, "Error (azure_close 2): %s\n", e.what());
        fflush(stderr);
        return -1;
      }
      delete bctxt->m_buf;
    }
  }
  delete bctxt;
  return 0;
}

//extern "C" DAL azure_dal_init(xmlNode *root, DAL_location max_loc);

DAL azure_dal_init(xmlNode *root, DAL_location max_loc)
{
  // first, calculate the number of digits required for pod/cap/block/scatter
  int d_pod = num_digits(max_loc.pod);
  if (d_pod < 1)
  {
    errno = EDOM;
    fprintf(stderr, "detected an inappropriate value for maximum pod: %d\n", max_loc.pod);
    fflush(stderr);
    return NULL;
  }
  int d_cap = num_digits(max_loc.cap);
  if (d_cap < 1)
  {
    errno = EDOM;
    fprintf(stderr, "detected an inappropriate value for maximum cap: %d\n", max_loc.cap);
    fflush(stderr);
    return NULL;
  }
  int d_block = num_digits(max_loc.block);
  if (d_block < 1)
  {
    errno = EDOM;
    fprintf(stderr, "detected an inappropriate value for maximum block: %d\n", max_loc.block);
    fflush(stderr);
    return NULL;
  }
  int d_scatter = num_digits(max_loc.scatter);
  if (d_scatter < 1)
  {
    errno = EDOM;
    fprintf(stderr, "detected an inappropriate value for maximum scatter: %d\n", max_loc.scatter);
    fflush(stderr);
    return NULL;
  }

  // make sure we start on a 'hostname' node
  if (root->type == XML_ELEMENT_NODE && strncmp((char *)root->name, "connection_string", 18) == 0)
  {

    // make sure that node contains a text element within it
    if (root->children != NULL && root->children->type == XML_TEXT_NODE)
    {

      // allocate space for our context struct
      AZURE_DAL_CTXT dctxt = new azure_dal_context_struct;
      if (dctxt == NULL)
      {
        fprintf(stderr, "failed to allocate dctxt\n");
        fflush(stderr);
        return NULL;
      } // malloc will set errno

      dctxt->max_loc = max_loc;

      size_t io_size = IO_SIZE;

      // find the access key, secret key, and region. Fail if any are missing
      while (root != NULL)
      {
        if (root->type == XML_ELEMENT_NODE && strncmp((char *)root->name, "connection_string", 18) == 0)
        {
          dctxt->connectionString = strdup((char *)root->children->content);
        }
        else if (root->type == XML_ELEMENT_NODE && strncmp((char *)root->name, "io_size", 8) == 0)
        {
          io_size = atol((char *)root->children->content);
        }
        root = root->next;
      }

      if (dctxt->connectionString == NULL)
      {
        fprintf(stderr, "no connection string\n");
        fflush(stderr);
        delete dctxt;
        errno = EINVAL;
        return NULL;
      }

      try
      {
        // Define the connection-string with your values.
        const utility::string_t storage_connection_string(U(dctxt->connectionString));

        // Retrieve storage account from connection string.
        azure::storage::cloud_storage_account storage_account = azure::storage::cloud_storage_account::parse(storage_connection_string);

        // Create the blob client.
        dctxt->client = storage_account.create_cloud_blob_client();
      }
      catch (const std::exception &e)
      {
        fprintf(stderr, "Error (azure_dal_init): %s\n", e.what());
        fflush(stderr);
        delete dctxt;
        return NULL;
      }

      // allocate and populate a new DAL structure
      DAL adal = new DAL_struct;
      if (adal == NULL)
      {
        fprintf(stderr, "failed to allocate space for a DAL_struct\n");
        fflush(stderr);
        delete dctxt;
        return NULL;
      } // malloc will set errno
      adal->name = "azure";
      adal->ctxt = (DAL_CTXT)dctxt;
      adal->io_size = io_size;
      adal->verify = azure_verify;
      adal->migrate = azure_migrate;
      adal->open = azure_open;
      adal->set_meta = azure_set_meta;
      adal->get_meta = azure_get_meta;
      adal->put = azure_put;
      adal->get = azure_get;
      adal->abort = azure_abort;
      adal->close = azure_close;
      adal->del = azure_del;
      adal->stat = azure_stat;
      adal->cleanup = azure_cleanup;
      return adal;
    }
    else
    {
      fprintf(stderr, "the \"hostname\" node is expected to contain a template string\n");
      fflush(stderr);
    }
  }
  else
  {
    fprintf(stderr, "root node of config is expected to be \"connection_string\"\n");
    fflush(stderr);
  }
  errno = EINVAL;
  return NULL; // failure of any condition check fails the function
}