#ifndef _MAPPED_REGION_H_
#define _MAPPED_REGION_H_

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h> /* context_t */
#include <block.h> /* block_*_t */

extern void mapped_region_init (mapped_region_t * mapped_region, int protect, int flags);
extern void mapped_region_free (mapped_region_t * mapped_region);
extern unsigned char * mapped_region_get_addr (context_t * context, block_id_t * block_id);

#endif /* _MAPPED_REGION_H_ */
