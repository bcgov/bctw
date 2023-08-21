import { NextFunction, Request, Response } from 'express';
import { fn_get_user_id } from '../apis/user_api';
import {
  constructFunctionQuery,
  getRowResults,
  query,
} from '../database/query';
import { UserRequest } from '../types/userRequest';
import { ROUTE_AUDIENCES } from '../routes';

export const getRegistrationStatus = async (
  keycloak_guid: string
): Promise<boolean> => {
  const sql = constructFunctionQuery(fn_get_user_id, [keycloak_guid]);
  const { result } = await query(sql);
  const isRegistered =
    typeof getRowResults(result, fn_get_user_id, true) === 'number';

  return isRegistered;
};

export const authorizeRequest = async (
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> => {
  const user = (req as UserRequest).user;
  const { origin, keycloak_guid } = user;

  user.registered = await getRegistrationStatus(keycloak_guid);

  // Registered BCTW users can access all routes
  if (user.registered && origin === 'BCTW') {
    return next();
  }

  // A Route with no defined allowed audiences can only accept registered BCTW users
  const allowedAudiences = ROUTE_AUDIENCES[req.path];
  if (!allowedAudiences || allowedAudiences.length === 0) {
    res.status(403).send('Forbidden');
    return;
  }

  // If the route is allowed for any audience
  if (allowedAudiences.includes('ANY')) {
    return next();
  }

  // If the user's origin isn't included, or the user is from BCTW or SIMS and isn't registered, return a forbidden error
  if (
    !allowedAudiences.includes(origin) ||
    (!user.registered && (origin === 'BCTW' || origin === 'SIMS'))
  ) {
    res.status(403).send('Forbidden');
    return;
  }

  next();
};
