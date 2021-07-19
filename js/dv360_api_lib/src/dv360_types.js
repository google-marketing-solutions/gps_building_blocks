/**
 * @license
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @fileoverview This file encapsulates type definitions for subtypes of DV360
 * Resources that are accessible via the DV360 API. The bare minimum of all
 * required properties is implemented; refer to the API reference linked per
 * type for an exhaustive and up-to-date list of properties.
 */

/**
 * Defines possible statuses for a DV360 resource.
 * @enum {string}
 */
const Status = {
  ACTIVE: 'ENTITY_STATUS_ACTIVE',
  ARCHIVED: 'ENTITY_STATUS_ARCHIVED',
  DELETED: 'ENTITY_STATUS_SCHEDULED_FOR_DELETION',
  DRAFT: 'ENTITY_STATUS_DRAFT',
  PAUSED: 'ENTITY_STATUS_PAUSED',
  UNSPECIFIED: 'ENTITY_STATUS_UNSPECIFIED',
};

/** @const {{map: function(?string): !Status}} */
const StatusMapper = {
  /**
   * Converts a raw status string to a concrete `Status`. Returns
   * `Status.UNSPECIFIED` for null inputs or unknown status values.
   *
   * @param {?string} rawStatus The raw status to convert. Can be nullable
   * @return {!Status} The concrete `Status`
   */
  map: (rawStatus) => {
    if (rawStatus) {
      const status = rawStatus.replace('ENTITY_STATUS_', '');
      return Status[status] || Status.UNSPECIFIED;
    }
    return Status.UNSPECIFIED;
  },
};

/**
 * Defines possible targeting types for a DV360 targeting option.
 * @enum {string}
 */
const TargetingType = {
  GEO_REGION: 'TARGETING_TYPE_GEO_REGION',
  UNSPECIFIED: 'TARGETING_TYPE_UNSPECIFIED',
};

/** @const {{map: function(?string): !TargetingType}} */
const TargetingTypeMapper = {
  /**
   * Converts a raw targeting type string to a concrete `TargetingType`. Returns
   * `TargetingType.UNSPECIFIED` for null inputs or unknown values.
   *
   * @param {?string} rawType The raw targeting type to convert. Can be nullable
   * @return {!TargetingType} The concrete `TargetingType`
   */
  map: (rawType) => {
    if (rawType) {
      const type = rawType.replace('TARGETING_TYPE_', '');
      return TargetingType[type] || TargetingType.UNSPECIFIED;
    }
    return TargetingType.UNSPECIFIED;
  },
};

/**
 * Defines possible pacing periods for spending ad budgets.
 * @enum {string}
 */
const PacingPeriod = {
  DAILY: 'PACING_PERIOD_DAILY',
  FLIGHT: 'PACING_PERIOD_FLIGHT',
  UNSPECIFIED: 'PACING_PERIOD_UNSPECIFIED',
};

/** @const {{map: function(?string): ?PacingPeriod}} */
const PacingPeriodMapper = {
  /**
   * Converts a raw pacing period string to a concrete `PacingPeriod`.
   *
   * @param {?string} rawType The raw pacing period to convert. Can be nullable
   * @return {?PacingPeriod} The concrete `PacingPeriod`, or null for unknown
   *     or erroneous values
   */
  map: (rawType) => {
    if (rawType) {
      const type = rawType.replace('PACING_PERIOD_', '');
      return PacingPeriod[type] || null;
    }
    return null;
  },
};

/**
 * Defines frequency cap configuration for limiting display of ads.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/FrequencyCap
 *
 * @typedef {{
 *     unlimited: (boolean|undefined),
 *     timeUnit: (string|undefined),
 *     timeUnitCount: (number|undefined),
 *     maxImpressions: (number|undefined),
 * }}
 */
let FrequencyCap;

/** @const {{map: function(*): ?FrequencyCap}} */
const FrequencyCapMapper = {
  /**
   * Converts a resource object returned by the API into a concrete
   * `FrequencyCap` instance.
   *
   * @param {*} resource The API resource object
   * @return {?FrequencyCap} The concrete instance, or null if the resource
   *     did not contain the expected properties
   */
  map: (resource) => {
    if ((ObjectUtil.hasOwnProperties(resource, ['unlimited']) &&
         typeof resource['unlimited'] === 'boolean' &&
         resource['unlimited'] === true) ||

        (ObjectUtil.hasOwnProperties(
             resource, ['timeUnit', 'timeUnitCount', 'maxImpressions']) &&
         Number.isInteger(resource['timeUnitCount']) &&
         Number.isInteger(resource['maxImpressions']))

    ) {
      return /** @type {!FrequencyCap} */ (resource);
    }
    return null;
  },
};

/**
 * Defines the pacing configuration for spending ad budgets.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/Pacing
 *
 * @typedef {{
 *     pacingPeriod: !PacingPeriod,
 *     pacingType: string,
 *     dailyMaxMicros: (string|undefined),
 *     dailyMaxImpressions: (string|undefined),
 * }}
 */
let Pacing;

/** @const {{map: function(*): ?Pacing}} */
const PacingMapper = {
  /**
   * Converts a resource object returned by the API into a concrete `Pacing`
   * instance.
   *
   * @param {*} resource The API resource object
   * @return {?Pacing} The concrete instance, or null if the resource did not
   *     contain the expected properties
   */
  map: (resource) => {
    if (ObjectUtil.hasOwnProperties(resource, ['pacingPeriod', 'pacingType'])) {
      const pacingPeriod = resource['pacingPeriod'];
      const mappedPacingPeriod = PacingPeriodMapper.map(pacingPeriod);

      if (mappedPacingPeriod &&
          (mappedPacingPeriod !== PacingPeriod.DAILY ||
           ObjectUtil.hasOwnProperties(
               resource, [], ['dailyMaxMicros', 'dailyMaxImpressions']))) {
        return /** @type {!Pacing} */ (resource);
      }
    }
    return null;
  },
};

/**
 * Defines a performance goal configuration.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/PerformanceGoal
 *
 * @typedef {{
 *     performanceGoalType: string,
 *     performanceGoalAmountMicros: (string|undefined),
 *     performanceGoalPercentageMicros: (string|undefined),
 *     performanceGoalString: (string|undefined),
 * }}
 */
let PerformanceGoal;

/** @const {{map: function(*): ?PerformanceGoal}} */
const PerformanceGoalMapper = {
  /**
   * Converts a resource object returned by the API into a concrete
   * `PerformanceGoal` instance.
   *
   * @param {*} resource The API resource object
   * @return {?PerformanceGoal} The concrete instance, or null if the resource
   *     did not contain the expected properties
   */
  map: (resource) => {
    if (ObjectUtil.hasOwnProperties(
            resource, ['performanceGoalType'], [
              'performanceGoalAmountMicros',
              'performanceGoalPercentageMicros',
              'performanceGoalString',
            ]) &&
        Object.keys(
                  /** @type {!Object<string, string>} */ (resource))
                .length === 2) {
      return /** @type {!PerformanceGoal} */ (resource);
    }
    return null;
  },
};

/**
 * Defines a maximize spend oriented bidding strategy.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/BiddingStrategy#maximizespendbidstrategy
 *
 * @typedef {{
 *     performanceGoalType: string,
 *     maxAverageCpmBidAmountMicros: (string|undefined),
 *     customBiddingAlgorithmId: (string|undefined),
 * }}
 */
let MaxSpendBiddingStrategy;

/**
 * Defines a performance goal oriented bidding strategy.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/BiddingStrategy#performancegoalbidstrategy
 *
 * @typedef {{
 *     performanceGoalAmountMicros: string,
 *     performanceGoalType: string,
 *     maxAverageCpmBidAmountMicros: (string|undefined),
 *     customBiddingAlgorithmId: (string|undefined),
 * }}
 */
let PerformanceGoalBiddingStrategy;

/**
 * Defines configuration that determines the bid price.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/BiddingStrategy
 *
 * @typedef {{
 *     fixedBid: ({bidAmountMicros: string}|undefined),
 *     maximizeSpendAutoBid: (!MaxSpendBiddingStrategy|undefined),
 *     performanceGoalAutoBid: (!PerformanceGoalBiddingStrategy|undefined),
 * }}
 */
let BiddingStrategy;

/** @const {{map: function(*): ?BiddingStrategy}} */
const BiddingStrategyMapper = {
  /**
   * Converts a resource object returned by the API into a concrete
   * `BiddingStrategy` instance.
   *
   * @param {*} resource The API resource object
   * @return {?BiddingStrategy} The concrete instance, or null if the resource
   *     did not contain the expected properties
   */
  map: (resource) => {
    if (ObjectUtil.hasOwnProperties(
            resource, [],
            ['fixedBid', 'maximizeSpendAutoBid', 'performanceGoalAutoBid'])) {
      const fixedBidStrategy = resource['fixedBid'];
      const maxSpendStrategy = resource['maximizeSpendAutoBid'];
      const performanceGoalStrategy = resource['performanceGoalAutoBid'];

      const validFixedBidStrategy = fixedBidStrategy &&
          ObjectUtil.hasOwnProperties(fixedBidStrategy, ['bidAmountMicros']);
      const validMaxSpendStrategy = maxSpendStrategy &&
          ObjectUtil.hasOwnProperties(
              maxSpendStrategy, ['performanceGoalType']);
      const validPerformanceGoalStrategy = performanceGoalStrategy &&
          ObjectUtil.hasOwnProperties(
              performanceGoalStrategy,
              ['performanceGoalType', 'performanceGoalAmountMicros']);

      if (validFixedBidStrategy || validMaxSpendStrategy ||
          validPerformanceGoalStrategy) {
        return /** @type {!BiddingStrategy} */ (resource);
      }
    }
    return null;
  },
};

/**
 * Defines general configuration for advertisers.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers#advertisergeneralconfig
 *
 * @typedef {{domainUrl: string, currencyCode: string}}
 */
let AdvertiserGeneralConfig;

/** @const {{map: function(*): ?AdvertiserGeneralConfig}} */
const AdvertiserGeneralConfigMapper = {
  /**
   * Converts a resource object returned by the API into a concrete
   * `AdvertiserGeneralConfig` instance.
   *
   * @param {*} resource The API resource object
   * @return {?AdvertiserGeneralConfig} The concrete instance, or null if the
   *     resource did not contain the expected properties
   */
  map: (resource) => {
    if (ObjectUtil.hasOwnProperties(
            resource, ['domainUrl', 'currencyCode'])) {
      return /** @type {!AdvertiserGeneralConfig} */ (resource);
    }
    return null;
  },
};

/**
 * Defines ad server configuration for advertisers.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers#advertiseradserverconfig
 *
 * @typedef {{
 *     thirdPartyOnlyConfig: ({
 *         pixelOrderIdReportingEnabled: (boolean|undefined),
 *     }|undefined),
 *     cmHybridConfig: ({
 *         cmAccountId: string,
 *         cmFloodlightConfigId: string,
 *         cmFloodlightLinkingAuthorized: boolean,
 *     }|undefined),
 * }}
 */
let AdvertiserAdServerConfig;

/** @const {{map: function(*): ?AdvertiserAdServerConfig}} */
const AdvertiserAdServerConfigMapper = {
  /**
   * Converts a resource object returned by the API into a concrete
   * `AdvertiserAdServerConfig` instance.
   *
   * @param {*} resource The API resource object
   * @return {?AdvertiserAdServerConfig} The concrete instance, or null if the
   *     resource did not contain the expected properties
   */
  map: (resource) => {
    if (ObjectUtil.hasOwnProperties(
            resource, [], ['thirdPartyOnlyConfig', 'cmHybridConfig'])) {
      const thirdPartyOnlyConfig = resource['thirdPartyOnlyConfig'];
      const cmHybridConfig = resource['cmHybridConfig'];

      let validThirdPartyOnlyConfig =
          ObjectUtil.isObject(thirdPartyOnlyConfig);

      if (validThirdPartyOnlyConfig) {
        const thirdPartyOnlyConfigKeys = Object.keys(
            /** @type {!Object<string, *>} */(thirdPartyOnlyConfig));

        validThirdPartyOnlyConfig = thirdPartyOnlyConfigKeys.length === 0 ||
            (thirdPartyOnlyConfigKeys.length === 1 &&
             ObjectUtil.hasOwnProperties(
                 thirdPartyOnlyConfig, ['pixelOrderIdReportingEnabled']) &&
             typeof thirdPartyOnlyConfig['pixelOrderIdReportingEnabled'] ===
                 'boolean');
      }
      const validCmHybridConfig = ObjectUtil.hasOwnProperties(
          cmHybridConfig, [
            'cmAccountId',
            'cmFloodlightConfigId',
            'cmFloodlightLinkingAuthorized',
          ]) &&
          typeof cmHybridConfig['cmFloodlightLinkingAuthorized'] === 'boolean';

      if (validThirdPartyOnlyConfig || validCmHybridConfig) {
        return /** @type {!AdvertiserAdServerConfig} */ (resource);
      }
    }
    return null;
  },
};

/**
 * Defines a campaign's flight configuration.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.campaigns#campaignflight
 *
 * @typedef {{
 *     plannedDates: {
 *         startDate: !ApiDate,
 *     },
 * }}
 */
let CampaignFlight;

/**
 * @const {{
 *     map: function(*): ?CampaignFlight,
 *     toJson: function(!CampaignFlight): !Object<string, *>,
 * }}
 */
const CampaignFlightMapper = {
  /**
   * Converts a resource object returned by the API into a concrete
   * `CampaignFlight` instance.
   *
   * @param {*} resource The API resource object
   * @return {?CampaignFlight} The concrete instance, or null if the resource
   *     did not contain the expected properties
   */
  map: (resource) => {
    if (ObjectUtil.hasOwnProperties(resource, ['plannedDates']) &&
        ObjectUtil.hasOwnProperties(resource['plannedDates'], ['startDate'])) {
      const apiStartDate =
          ApiDate.fromApiResource(resource['plannedDates']['startDate']);

      if (apiStartDate) {
        resource['plannedDates']['startDate'] = apiStartDate;
        return /** @type {!CampaignFlight} */ (resource);
      }
    }
    return null;
  },

  /**
   * Converts a `CampaignFlight` to its expected JSON representation.
   *
   * @param {!CampaignFlight} flight The flight to convert
   * @return {!Object<string, *>} The custom JSON representation of the
   *     `CampaignFlight`
   */
  toJson: (flight) => {
    return {
      plannedDates: {
        startDate: flight.plannedDates.startDate.toJSON(),
      },
    };
  },
};

/**
 * Defines a campaign's goal configuration.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.campaigns#campaigngoal
 *
 * @typedef {{
 *     campaignGoalType: string,
 *     performanceGoal: !PerformanceGoal,
 * }}
 */
let CampaignGoal;

/** @const {{map: function(*): ?CampaignGoal}} */
const CampaignGoalMapper = {
  /**
   * Converts a resource object returned by the API into a concrete
   * `CampaignGoal` instance.
   *
   * @param {*} resource The API resource object
   * @return {?CampaignGoal} The concrete instance, or null if the resource did
   *     not contain the expected properties
   */
  map: (resource) => {
    if (ObjectUtil.hasOwnProperties(
            resource, ['campaignGoalType', 'performanceGoal']) &&
        PerformanceGoalMapper.map(resource['performanceGoal'])) {
      return /** @type {!CampaignGoal} */ (resource);
    }
    return null;
  },
};

/**
 * Defines an insertion order's budget segment configuration.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.insertionOrders#InsertionOrderBudgetSegment
 *
 * @typedef {{
 *     budgetAmountMicros: string,
 *     dateRange: {
 *         startDate: !ApiDate,
 *         endDate: !ApiDate,
 *     },
 * }}
 */
let InsertionOrderBudgetSegment;

/**
 * @const {{
 *     map: function(*): ?InsertionOrderBudgetSegment,
 *     toJson: function(!InsertionOrderBudgetSegment): !Object<string, *>,
 * }}
 */
const InsertionOrderBudgetSegmentMapper = {
  /**
   * Converts a resource object returned by the API into a concrete
   * `InsertionOrderBudgetSegment` instance.
   *
   * @param {*} resource The API resource object
   * @return {?InsertionOrderBudgetSegment} The concrete instance, or null if
   *     the resource did not contain the expected properties
   */
  map: (resource) => {
    if (ObjectUtil.hasOwnProperties(
            resource, ['budgetAmountMicros', 'dateRange'])) {
      const dateRange = resource['dateRange'];
      const startDate = ApiDate.fromApiResource(dateRange['startDate']);
      const endDate = ApiDate.fromApiResource(dateRange['endDate']);

      if (startDate && endDate) {
        dateRange['startDate'] = startDate;
        dateRange['endDate'] = endDate;

        return /** @type {!InsertionOrderBudgetSegment} */ (resource);
      }
    }
    return null;
  },

  /**
   * Converts an `InsertionOrderBudgetSegment` to its expected JSON
   * representation.
   *
   * @param {!InsertionOrderBudgetSegment} segment The segment to convert
   * @return {!Object<string, *>} The custom JSON representation of the
   *     `InsertionOrderBudgetSegment`
   */
  toJson: (segment) => {
    return {
      budgetAmountMicros: segment.budgetAmountMicros,
      dateRange: {
        startDate: segment.dateRange.startDate.toJSON(),
        endDate: segment.dateRange.endDate.toJSON(),
      },
    };
  },
};

/**
 * Defines an insertion order's budget configuration.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.insertionOrders#InsertionOrderBudget
 *
 * @typedef {{
 *     budgetUnit: string,
 *     budgetSegments: !Array<!InsertionOrderBudgetSegment>,
 * }}
 */
let InsertionOrderBudget;

/**
 * @const {{
 *     map: function(*): ?InsertionOrderBudget,
 *     toJson: function(!InsertionOrderBudget): !Object<string, *>,
 * }}
 */
const InsertionOrderBudgetMapper = {
  /**
   * Converts a resource object returned by the API into a concrete
   * `InsertionOrderBudget` instance.
   *
   * @param {*} resource The API resource object
   * @return {?InsertionOrderBudget} The concrete instance, or null if the
   *     resource did not contain the expected properties
   */
  map: (resource) => {
    if (ObjectUtil.hasOwnProperties(
            resource, ['budgetUnit', 'budgetSegments'])) {
      const budgetSegments = resource['budgetSegments'];

      if (Array.isArray(budgetSegments) && budgetSegments.length !== 0) {
        let valid = true;

        budgetSegments.forEach((segment) => {
          const mappedSegment = InsertionOrderBudgetSegmentMapper.map(segment);
          valid = valid && mappedSegment;
        });
        if (valid) {
          return /** @type {!InsertionOrderBudget} */ (resource);
        }
      }
    }
    return null;
  },

  /**
   * Converts an `InsertionOrderBudget` to its expected JSON representation.
   *
   * @param {!InsertionOrderBudget} budget The budget to convert
   * @return {!Object<string, *>} The custom JSON representation of the
   *     `InsertionOrderBudget`
   */
  toJson: (budget) => {
    const segments = budget.budgetSegments.map(
        (segment) => InsertionOrderBudgetSegmentMapper.toJson(segment));

    return {
      budgetUnit: budget.budgetUnit,
      budgetSegments: segments,
    };
  },
};

/**
 * Defines line item flight configuration.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.lineItems#LineItemFlight
 *
 * @typedef {{
 *     flightDateType: string,
 *     triggerId: (string|undefined),
 *     dateRange: ({
 *         startDate: !ApiDate,
 *         endDate: !ApiDate,
 *     }|undefined)
 * }}
 */
let LineItemFlight;

/**
 * @const {{
 *     map: function(*): ?LineItemFlight,
 *     toJson: function(!LineItemFlight): !Object<string, *>,
 * }}
 */
const LineItemFlightMapper = {
  /**
   * Converts a resource object returned by the API into a concrete
   * `LineItemFlight` instance.
   *
   * @param {*} resource The API resource object
   * @return {?LineItemFlight} The concrete instance, or null if the resource
   *     did not contain the expected properties
   */
  map: (resource) => {
    if (ObjectUtil.hasOwnProperties(resource, ['flightDateType'])) {
      const dateRange = resource['dateRange'];
      let validDateRange = false;

      if (dateRange) {
        const startDate = ApiDate.fromApiResource(dateRange['startDate']);
        const endDate = ApiDate.fromApiResource(dateRange['endDate']);

        if (startDate && endDate) {
          dateRange['startDate'] = startDate;
          dateRange['endDate'] = endDate;
          validDateRange = true;
        }
      }
      if (!dateRange || validDateRange) {
        return /** @type {!LineItemFlight} */ (resource);
      }
    }
    return null;
  },

  /**
   * Converts a `LineItemFlight` to its expected JSON representation.
   *
   * @param {!LineItemFlight} flight The flight to convert
   * @return {!Object<string, *>} The custom JSON representation of the
   *     `LineItemFlight`
   */
  toJson: (flight) => {
    if (!flight.dateRange) {
      return flight;
    }
    const result = {
      flightDateType: flight.flightDateType,
      dateRange: {
        startDate: flight.dateRange.startDate.toJSON(),
        endDate: flight.dateRange.endDate.toJSON(),
      },
    };
    if (flight.triggerId) {
      result['triggerId'] = flight.triggerId;
    }
    return result;
  },
};

/**
 * Defines line item budget configuration.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.lineItems#LineItemBudget
 *
 * @typedef {{
 *     budgetAllocationType: string,
 * }}
 */
let LineItemBudget;

/** @const {{map: function(*): ?LineItemBudget}} */
const LineItemBudgetMapper = {
  /**
   * Converts a resource object returned by the API into a concrete
   * `LineItemBudget` instance.
   *
   * @param {*} resource The API resource object
   * @return {?LineItemBudget} The concrete instance, or null if the resource
   *     did not contain the expected properties
   */
  map: (resource) => {
    if (ObjectUtil.hasOwnProperties(resource, ['budgetAllocationType'])) {
      return /** @type {!LineItemBudget} */ (resource);
    }
    return null;
  },
};

/**
 * Defines line item partner revenue model configuration.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.lineItems#PartnerRevenueModel
 *
 * @typedef {{
 *     markupType: string,
 *     markupAmount: (string|undefined),
 * }}
 */
let LineItemPartnerRevenueModel;

/** @const {{map: function(*): ?LineItemPartnerRevenueModel}} */
const LineItemPartnerRevenueModelMapper = {
  /**
   * Converts a resource object returned by the API into a concrete
   * `LineItemPartnerRevenueModel` instance.
   *
   * @param {*} resource The API resource object
   * @return {?LineItemPartnerRevenueModel} The concrete instance, or null if
   *     the resource did not contain the expected properties
   */
  map: (resource) => {
    if (ObjectUtil.hasOwnProperties(resource, ['markupType'])) {
      return /** @type {!LineItemPartnerRevenueModel} */ (resource);
    }
    return null;
  },
};

/**
 * Class representing a date as it is provided by the DV360 API. Note:
 * individual values are not padded (i.e. 1 is valid for day or month) and may
 * be 0 to indicate 'ignore value' (e.g. 0 for day means a year and month
 * representation without a specific day).
 */
class ApiDate {
  /**
   * Constructs an instance of `ApiDate`.
   *
   * @param {number} year The year to set
   * @param {number} month The month to set
   * @param {number} day The day to set
   */
  constructor(year, month, day) {

    /** @private @const {number} */
    this.year_ = year;

    /** @private @const {number} */
    this.month_ = month;

    /** @private @const {number} */
    this.day_ = day;
  }

  /**
   * Converts a resource object returned by the API to `ApiDate` if it matches
   * the type. Returns null for any invalid input.
   *
   * @param {*} rawDate The raw object to convert. Can be null or undefined
   * @return {?ApiDate} The concrete `ApiDate`, or null if invalid
   */
  static fromApiResource(rawDate) {
    if (ObjectUtil.hasOwnProperties(rawDate, ['year', 'month', 'day']) &&
        Number.isInteger(rawDate['year']) &&
        Number.isInteger(rawDate['month']) &&
        Number.isInteger(rawDate['day'])) {
      return new ApiDate(
          Number(rawDate['year']), Number(rawDate['month']),
          Number(rawDate['day']));
    }
    return null;
  }

  /**
   * Returns a new `ApiDate` for the current date.
   *
   * @return {!ApiDate}
   */
  static now() {
    const date = new Date();

    return new ApiDate(date.getFullYear(), date.getMonth() + 1, date.getDate());
  }

  /**
   * Returns all properties of this `ApiDate` that are modifiable.
   *
   * @param {string=} prefix Optional prefix for the properties. Defaults to an
   *     empty string
   * @return {!Array<string>} An array of properties that are modifiable
   */
  static getMutableProperties(prefix = '') {
    return [`${prefix}year`, `${prefix}month`, `${prefix}day`];
  }

  /**
   * Compares this `ApiDate` to 'other' and returns an `Array` of changed
   * properties.
   *
   * @param {?ApiDate} other The other api date to compare
   * @param {string=} prefix Optional prefix for the changed properties.
   *     Defaults to an empty string
   * @return {!Array<string>} An array of changed mutable properties between
   *     this and 'other'
   */
  getChangedProperties(other, prefix = '') {
    const changedProperties = [];

    if (other) {
      if (this.getYear() !== other.getYear()) {
        changedProperties.push(`${prefix}year`);
      }
      if (this.getMonth() !== other.getMonth()) {
        changedProperties.push(`${prefix}month`);
      }
      if (this.getDay() !== other.getDay()) {
        changedProperties.push(`${prefix}day`);
      }
    } else {
      return ApiDate.getMutableProperties(prefix);
    }
    return changedProperties;
  }

  /**
   * Converts this instance of `ApiDate` to its expected JSON representation.
   * This method is called by default when an instance of `ApiDate` is passed
   * to `JSON.stringify`.
   *
   * @return {!Object<string, number>} The custom JSON representation of this
   *     `ApiDate` instance
   */
  toJSON() {
    return {
      year: this.getYear(),
      month: this.getMonth(),
      day: this.getDay(),
    };
  }

  /**
   * Returns the year.
   *
   * @return {number}
   */
  getYear() {
    return this.year_;
  }

  /**
   * Returns the month.
   *
   * @return {number}
   */
  getMonth() {
    return this.month_;
  }

  /**
   * Returns the day.
   *
   * @return {number}
   */
  getDay() {
    return this.day_;
  }
}
