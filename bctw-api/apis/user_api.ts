import { pgPool, to_pg_str, QueryResultCbFn } from '../pg';
import { User, UserRole } from '../types/user'

const addUser = function(user: User, userRole: UserRole, onDone: QueryResultCbFn): void {
  const sql = `select * from bctw.add_user('${JSON.stringify(user)}', ${to_pg_str(userRole)})`;
  return pgPool.query(sql, onDone);
}

// const updateUser = function() {}
// const logout = function() {}

/*
  for updating a user's system role. ex granting another user admin
*/
// const updateSystemUserPermission = function() {}

/* 
  - needs to have admin role?
*/
// const deleteUser = function() {}

const getUserRole = function(idir: string, onDone: QueryResultCbFn): void {
  if (!idir) {
    throw('IDIR must be supplied')
  }
  const sql = `select bctw.get_user_role('${idir}');`
  return pgPool.query(sql, onDone);
}

const getUserCollars = function(idir: string, onDone: QueryResultCbFn): void {
  if (!idir) {
    throw('IDIR must be supplied')
  }
  const sql = `select bctw.get_collars('${idir}');`
  return pgPool.query(sql, onDone);
}

export {
  addUser,
  getUserRole,
  getUserCollars
}
