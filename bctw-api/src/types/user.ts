import { BCTWBaseType } from './base_types';

interface IUserInput {
  user_id: string,
  idir: string,
  bceid: string,
  email: string,
}

type User = BCTWBaseType & IUserInput;

// used to represent user role type
enum UserRole {
  administrator = 'administrator',
  owner = 'owner',
  observer = 'observer'
}

enum eCritterPermission {
  view = 'view',
  change = 'change'
}

export {
  IUserInput,
  User,
  UserRole,
  eCritterPermission,
} 