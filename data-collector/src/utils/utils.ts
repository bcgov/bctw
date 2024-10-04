/**
 * Converts an object's keys to lowercase, preserving its values.
 * Used in this module as JSON objects received from vendor APIs have
 * camelCase keys, and the BCTW database has all lowercase keys.
 */
export const toLowerCaseObjectKeys = <T extends Record<string, any>>(rec: T): Record<string, any> => {
  const ret: Record<string, any> = {};

  for (const key in rec) {
    if (rec.hasOwnProperty(key)) {
      const lower = key.toLowerCase();
      ret[lower] = rec[key];
    }
  }

  return ret;
};
