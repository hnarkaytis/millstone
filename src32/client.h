#ifndef _CLIENT_H_
#define _CLIENT_H_

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif /* HAVE_CONFIG_H */
#include <millstone.h>

#include <metaresc.h>

TYPEDEF_STRUCT (client_id_t, unsigned short int id)

status_t run_client (config_t *);

#endif /* _CLIENT_H_ */
