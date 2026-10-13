/*-------------------------------------------------------------------------
 *
 * worlber_ha.h
 *		Shared declarations of the worlber_ha extension.
 *
 * contrib/worlber_ha/worlber_ha.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WORLBER_HA_H
#define WORLBER_HA_H

extern bool worlber_ha_fence;
extern int	worlber_ha_lease_ttl;

extern void worlber_ha_register_fence(void);

#endif							/* WORLBER_HA_H */
