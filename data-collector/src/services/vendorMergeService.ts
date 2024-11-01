import SQL from 'sql-template-strings';
import { IDBConnection } from '../database/db';
import { DBService } from './db-service';

/**
 * Class responsible for refreshing materialized views and merging vendor telemetry.
 */
export class VendorMergeService extends DBService {
  /**
   * Constructor for the VendorMergeService class.
   * @param connection - The database connection instance.
   */
  constructor(connection: IDBConnection) {
    super(connection);
  }

  /**
   * Main processing method to execute the vendor merge operations.
   * It performs a VACUUM and refreshes the telemetry materialized views concurrently.
   */
  async process(): Promise<void> {
    try {
      await Promise.all([this._vacuum(), this._refreshHistoricalTelemetry()]);
      console.log('Vendor merge operations completed successfully.');
    } catch (error) {
      console.error('Error during vendor merge process: ', error);
    }
  }

  /**
   * Executes a VACUUM ANALYZE command to clean up the database.
   * This helps optimize performance by reclaiming storage.
   */
  private async _vacuum(): Promise<void> {
    try {
      await this.connection.sql(SQL`VACUUM ANALYZE;`);
      console.log('VACUUM ANALYZE completed successfully.');
    } catch (error) {
      console.error('Error during VACUUM ANALYZE: ', error);
    }
  }

  /**
   * Refreshes the materialized view for telemetry data.
   * This updates the view with the latest data.
   */
  private async _refreshHistoricalTelemetry(): Promise<void> {
    try {
      const sql = SQL`CREATE OR REPLACE MATERIALIZED VIEW telemetry AS 
      SELECT * FROM api_vectronic_telemetry`;
      await this.connection.sql(sql);
      console.log('Telemetry materialized view refreshed successfully.');
    } catch (error) {
      console.error('Error during refreshing telemetry materialized view: ', error);
    }
  }
}
