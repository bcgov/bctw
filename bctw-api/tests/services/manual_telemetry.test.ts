import {
  IManualTelemetry,
  ManualTelemetryService,
} from '../../src/services/manual_telemetry';
import { apiError } from '../../src/utils/error';
import { mockQuery } from '../apis/test_helpers';

const mockService = new ManualTelemetryService('UUID');
const mockTelemetry: IManualTelemetry[] = [
  {
    telemetry_manual_id: 'a',
    deployment_id: 'b',
    latitude: 1,
    longitude: 2,
    date: new Date(),
  },
];

const mockQueryReturn: any = {
  result: {
    rows: mockTelemetry,
  },
  error: new Error('TEST'),
  isError: false,
};

describe('manual telemetry service', () => {
  describe('constructor', () => {
    it('should throw error if no uuid provided to constructor', () => {
      expect(() => {
        new ManualTelemetryService();
      }).toThrowError();
    });
    it('should not throw error and set keycloak_guid with uuid passed', () => {
      expect(mockService.keycloak_guid).toBe('UUID');
    });
  });

  describe('private validation methods', () => {
    describe('_validateUuidArray', () => {
      it('throws if no uuids provided', () => {
        expect(() => {
          mockService._validateUuidArray([]);
        }).toThrowError();
      });

      it('throws if not array', () => {
        expect(() => {
          mockService._validateUuidArray({} as Array<string>);
        }).toThrowError();
      });

      it('throws if not all items in array are strings', () => {
        expect(() => {
          mockService._validateUuidArray(['a', 1]);
        }).toThrowError();
      });

      it('does not throw if valid data', () => {
        expect(() => {
          mockService._validateUuidArray(['a', 'b']);
        }).not.toThrowError();
      });
    });

    describe('_validateManualTelemetryCreate', () => {
      it('throws if no telemetry provided', () => {
        expect(() => {
          mockService._validateManualTelemetryCreate([]);
        }).toThrowError();
      });

      it('throws if missing deployment_id', () => {
        const { deployment_id, ...t } = mockTelemetry[0];
        expect(() => {
          mockService._validateManualTelemetryCreate([t]);
        }).toThrowError();
      });

      it('throws if missing deployment_id', () => {
        const { deployment_id, ...t } = mockTelemetry[0];
        expect(() => {
          mockService._validateManualTelemetryCreate([t]);
        }).toThrowError();
      });

      it('throws if missing latitude or longitude', () => {
        const { latitude, longitude, ...t } = mockTelemetry[0];
        expect(() => {
          mockService._validateManualTelemetryCreate([t]);
        }).toThrowError();
      });

      it('throws if missing date', () => {
        const { date, ...t } = mockTelemetry[0];
        expect(() => {
          mockService._validateManualTelemetryCreate([t]);
        }).toThrowError();
      });

      it('does not throw if valid data', () => {
        expect(() => {
          mockService._validateManualTelemetryCreate(mockTelemetry);
        }).not.toThrowError();
      });
    });
    describe('_validateManualTelemetryPatch', () => {
      it('throws if missing telemetry_manual_id', () => {
        const { telemetry_manual_id, ...t } = mockTelemetry[0];
        expect(() => {
          mockService._validateManualTelemetryPatch([t]);
        }).toThrowError();
      });

      it('throws if only 1 property', () => {
        expect(() => {
          mockService._validateManualTelemetryPatch([
            { telemetry_manual_id: 'a' },
          ]);
        }).toThrowError();
      });
    });

    describe('getManualTelemetry', () => {
      it('should return telemetry if no error', async () => {
        mockQuery.mockResolvedValue(mockQueryReturn);
        const telemetry = await mockService.getManualTelemetry();
        expect(telemetry).toBe(mockTelemetry);
      });

      it('should throw apiError with status 500 if query has error', async () => {
        mockQuery.mockResolvedValue({ ...mockQueryReturn, isError: true });
        try {
          await mockService.getManualTelemetry();
        } catch (err) {
          expect(err).toBeDefined();
          expect(err).toBeInstanceOf(apiError);
        }
      });
    });
  });
});
