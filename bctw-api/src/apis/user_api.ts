import { pgPool, to_pg_str, QueryResultCbFn, to_pg_date, getRowResults } from '../pg';
import { User, UserRole } from '../types/user'
import { transactionify, isProd } from '../pg';
import { Request, Response } from 'express';

const _addUser = function(user: User, userRole: UserRole, onDone: QueryResultCbFn): void {
  let sql = `select * from bctw.add_user('${JSON.stringify(user)}', ${to_pg_str(userRole)});`;
  if (!isProd) {
    sql = `begin;\n ${sql}\n select * from bctw.user where idir=${to_pg_str(user.idir)}; rollback;`
  }
  return pgPool.query(sql, onDone);
}

/* ## addUser
  - idir must be unique
  - todo: user adding must be admin?
  todo: test
*/
const addUser = async function(req: Request, res: Response): Promise<void> {
  const params = req.body;
  const user: User = params.user;
  const role: string = params.role;

  const done = function (err, data) {
    if (err) {
      return res.status(500).send(`Failed to query database: ${err}`);
    }
    if (!isProd) {
      const results = data.filter(obj => obj.command === 'SELECT' && obj.rows.length)
      const userObj = results[results.length -1]
      console.log(`user added: ${JSON.stringify(userObj.rows[0])}`)
    }
    res.send(`user ${user.idir} added sucessfully`);
  };
  await _addUser(user, role as UserRole, done);
}

const _getUserRole = function(idir: string, onDone: QueryResultCbFn): void {
  if (!idir) {
    return onDone(Error('IDIR must be supplied'), null);
  }
  const sql = `select bctw.get_user_role('${idir}');`
  return pgPool.query(sql, onDone);
}

/* ## getRole todo: test
*/
const getUserRole = async function (req: Request, res: Response): Promise<void> {
  const idir = (req?.query?.idir || '') as string;
  const done = function (err, data) {
    if (err) {
      return res.status(500).send(`Failed to query database: ${err}`);
    }
    const results = data.rows.map(row => row['get_user_role'])
    if (results && results.length) {
      res.send(results[0]);
    }
  };
  await _getUserRole(idir, done)
}

const _assignCritterToUser = function(
  idir: string,
  animalId: string,
  start: Date,
  end: Date,
  onDone: QueryResultCbFn
): void {
  if (!idir) {
    return onDone(Error('IDIR must be supplied'), null);
  }
  const sql = transactionify(`select bctw.link_animal_to_user(
    ${to_pg_str(idir)},
    ${to_pg_str(animalId)},
    ${to_pg_date(end)},
    ${to_pg_date(start)}
  )`);
  return pgPool.query(sql, onDone);
}

const assignCritterToUser = async function(req: Request, res:Response): Promise<void> {
  const idir = (req?.query?.idir || '') as string;
  const body = req.body;
  const done = function (err, data) {
    if (err) {
      return res.status(500).send(`Failed to query database: ${err}`);
    }
    const results = getRowResults(data, 'link_animal_to_user');
    res.send(results);
  };
  await _assignCritterToUser(
    idir,
    body.animalId,
    body.start,
    body.end,
    done
  )
}

export {
  addUser,
  getUserRole,
  assignCritterToUser
}
