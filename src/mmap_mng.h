#ifndef _MMAP_MNG_H_
#define _MMAP_MNG_H_

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h> /* context_t */
#include <block.h> /* block_*_t */

extern void mmap_mng_init (mmap_mng_t * mmap_mng, int protect, int flags);
extern unsigned char * mmap_mng_get_addr (context_t * context, block_id_t * block_id);
extern void mmap_mng_unref (mmap_mng_t * mmap_mng, block_id_t * block_id);
extern status_t mmap_mng_lock (mmap_mng_t * mmap_mng, block_id_t * block_id);
extern void mmap_mng_free (mmap_mng_t * mmap_mng);

#endif /* _MMAP_MNG_H_ */
