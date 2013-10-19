#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <mtu_tune.h>
#include <block.h>

#include <metaresc.h>

int
block_id_compar (const mr_ptr_t x, const mr_ptr_t y, const void * context)
{
  block_id_t * x_ = x.ptr;
  block_id_t * y_ = y.ptr;
  int cmp = (x_->offset > y_->offset) - (x_->offset < y_->offset);
  if (cmp)
    return (cmp);
  cmp = (x_->size > y_->size) - (x_->size < y_->size);
  return (cmp);
}

mr_hash_value_t
block_id_hash (const mr_ptr_t x, const void * context)
{
  block_id_t * x_ = x.ptr;
  return (x_->offset >> MIN_TRANSFER_BLOCK_SIZE_BITS);
}

void
block_id_free (const mr_ptr_t x, const void * context)
{
  MR_FREE (x.ptr);
}
