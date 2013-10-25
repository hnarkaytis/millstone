#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h>
#include <logging.h>
#include <block.h>
#include <connection.h>
#include <mtu_tune.h>

#include <stddef.h> /* size_t, ssize_t */
#include <string.h> /* memset, strerror */
#include <limits.h> /* CHAR_BIT */
#include <stdbool.h> /* bool */

#define MTU_TUNE_LEVELUP_RATE (32)

int
mtu_tune_get_width (int value)
{
  int down = MIN_TRANSFER_BLOCK_SIZE_BITS;
  int up = MAX_TRANSFER_BLOCK_SIZE_BITS + 1;
  while (down != up)
    {
      int mid = (down + up) >> 1;
      int diff = (1 << mid) - value;
      if (0 == diff)
	return (mid);
      if (diff > 0)
	up = mid;
      else
	down = mid + 1;
    }
  return (down + 1);
}

int
bit_count_ (uint64_t value)
{
  int count;
  for (count = 0; value != 0; ++count)
    value &= value - 1;
  return (count);
}

int
bit_count (uint64_t value)
{
  static bool inited = FALSE;
  static int byte_bits[1 << CHAR_BIT];
  int i, count;

  if (!inited)
    {
      for (i = 0; i < sizeof (byte_bits) / sizeof (byte_bits[0]); ++i)
	byte_bits[i] = bit_count_ (i);
      inited = TRUE;
    }

  for (count = 0; value != 0; value >>= CHAR_BIT)
    count += byte_bits[value & ((1 << CHAR_BIT) - 1)];
  
  return (count);
}

void
mtu_tune_init (mtu_tune_t * mtu_tune)
{
  int i;
  memset (mtu_tune, 0, sizeof (*mtu_tune));
  mtu_tune->current_mtu = mtu_tune_get_width (EXPECTED_PACKET_SIZE);
  for (i = 0; i < sizeof (mtu_tune->mtu_info) / sizeof (mtu_tune->mtu_info[0]); ++i)
    pthread_mutex_init (&mtu_tune->mtu_info[i].mutex, NULL);
  DUMP_VAR (mtu_tune_t, mtu_tune);
}

void
mtu_tune_log (mtu_tune_t * mtu_tune, size_t size, bool failure)
{
  int mtu = mtu_tune_get_width (size);
  if ((mtu < MIN_TRANSFER_BLOCK_SIZE_BITS) || (mtu > MAX_TRANSFER_BLOCK_SIZE_BITS))
    {
      ERROR_MSG ("Unexpected MTU size %zd mtu %d\n", size, mtu);
      return;
    }
  mtu_info_t * mtu_info = &mtu_tune->mtu_info[mtu];

  pthread_mutex_lock (&mtu_info->mutex);
  uint64_t mask = ((uint64_t)1) << (mtu_info->count_received++ & (CHAR_BIT * sizeof (mtu_info->log) - 1));
  if (!(mtu_info->log & mask) != !failure)
    {
      mtu_info->log ^= mask;
      if (failure)
	++mtu_info->count_errors;
      else
	--mtu_info->count_errors;
    }
  pthread_mutex_unlock (&mtu_info->mutex);
  
  if (mtu == mtu_tune->current_mtu)
    if (mtu_tune->current_mtu > MIN_TRANSFER_BLOCK_SIZE_BITS)
      if (mtu_info->count_errors > ((CHAR_BIT * sizeof (mtu_info->log)) >> 1))
	--mtu_tune->current_mtu;

  if ((mtu == mtu_tune->current_mtu + 1) && !failure)
    if (mtu_tune->current_mtu < MAX_TRANSFER_BLOCK_SIZE_BITS)
      if (mtu_info->count_errors <= ((CHAR_BIT * sizeof (mtu_info->log)) >> 1))
	mtu_tune->current_mtu = mtu;
}

void
mtu_tune_set_size (mtu_tune_t * mtu_tune, block_id_t * block_id)
{
  int mtu;
  for (mtu = MIN_TRANSFER_BLOCK_SIZE_BITS; mtu <= mtu_tune->current_mtu; ++mtu)
    if (block_id->offset & (1 << (mtu - 1)))
      break;
  if ((mtu > MAX_TRANSFER_BLOCK_SIZE_BITS) ||
      (block_id->offset & (1 << (mtu - 1))) ||
      (++mtu_tune->mtu_info[mtu].count_send_attempt % MTU_TUNE_LEVELUP_RATE != 0))
    --mtu;
  if (block_id->size > (1 << mtu))
    block_id->size = 1 << mtu;
}
