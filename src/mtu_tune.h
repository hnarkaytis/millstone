#ifndef _MTU_TUNE_H_
#define _MTU_TUNE_H_

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h> /* status_t */

#include <metaresc.h>

#define MAX_TRANSFER_BLOCK_SIZE_BITS (15)
#define MIN_TRANSFER_BLOCK_SIZE_BITS (9)
#define MIN_TRANSFER_BLOCK_SIZE (1 << MIN_TRANSFER_BLOCK_SIZE_BITS)
#define MAX_TRANSFER_BLOCK_SIZE (MIN_BLOCK_SIZE)

TYPEDEF_STRUCT (mtu_info_t,
		uint64_t log,
		int count_send_attempt,
		int count_received,
		)

TYPEDEF_STRUCT (mtu_tune_t,
		int current_mtu,
		(mtu_info_t, mtu_info, [MAX_TRANSFER_BLOCK_SIZE_BITS + 1]),
		)

extern void mtu_tune_init (mtu_tune_t * mtu_tune);
extern void mtu_tune_log (mtu_tune_t * mtu_tune, size_t size, bool failure);
extern void mtu_tune_set_size (mtu_tune_t * mtu_tune, block_id_t * block_id);

#endif /* _MTU_TUNE_H_ */
