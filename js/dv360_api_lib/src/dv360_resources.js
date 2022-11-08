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
 * @fileoverview This file encapsulates domain object representations for DV360
 * Resources that are accessible via the DV360 API. Static mappers are
 * implemented per domain object to ensure proper separation of concerns between
 * the library's domain objects and their expected API counterparts.
 */

/** A base class for DV360 resources that are accessible via the API. */
class DisplayVideoResource {
  /**
   * Constructs an instance of `DisplayVideoResource`.
   *
   * @param {?string} id The unique resource ID. Should be null for resources
   *     that are yet to be created by the API
   * @param {?string} displayName The display name. Can be null for certain
   *     resources
   * @param {!Status=} status Optional status to set
   */
  constructor(id, displayName, status = Status.UNSPECIFIED) {
    /** @private @const {?string} */
    this.id_ = id;

    /** @private {?string} */
    this.displayName_ = displayName;

    /** @private {!Status} */
    this.status_ = status;
  }

  /**
   * Compares this `DisplayVideoResource` to 'other' and returns an `Array` of
   * changed mutable properties (ID for example is immutable and cannot be
   * changed (it can only be "set" after an object has been created by the API),
   * therefore this method will not compare it between 'this' and 'other').
   * @see #getMutableProperties for a complete list of mutable properties.
   *
   * @param {?DisplayVideoResource} other The other resource to compare
   * @return {!Array<string>} An array of changed mutable properties between
   *     this and 'other'
   */
  getChangedProperties(other) {
    const changedProperties = [];

    if (!other) {
      changedProperties.push(...this.getMutableProperties());
    } else {
      if (this.getDisplayName() !== other.getDisplayName()) {
        changedProperties.push('displayName');
      }
      if (this.getStatus() !== other.getStatus()) {
        changedProperties.push('entityStatus');
      }
    }
    return changedProperties;
  }

  /**
   * Compares this `DisplayVideoResource` to 'other' and returns a
   * comma-separated string of changed mutable properties.
   *
   * @param {?DisplayVideoResource} other The other resource to compare
   * @return {string} A comma-separated string of changed mutable properties
   *     between this and 'other'
   */
  getChangedPropertiesString(other) {
    return this.getChangedProperties(other).join(',');
  }

  /**
   * Returns all properties of this `DisplayVideoResource` that are modifiable.
   *
   * @return {!Array<string>} An array of properties that are modifiable
   */
  getMutableProperties() {
    return ['displayName', 'entityStatus'];
  }

  /**
   * Returns the API resource ID.
   *
   * @return {?string}
   */
  getId() {
    return this.id_;
  }

  /**
   * Returns the API resource display name.
   *
   * @return {?string}
   */
  getDisplayName() {
    return this.displayName_;
  }

  /**
   * Sets the API resource display name.
   *
   * @param {string} displayName
   */
  setDisplayName(displayName) {
    this.displayName_ = displayName;
  }

  /**
   * Returns the API resource status.
   *
   * @return {!Status}
   */
  getStatus() {
    return this.status_;
  }

  /**
   * Sets the API resource status.
   *
   * @param {!Status} status
   */
  setStatus(status) {
    this.status_ = status;
  }
}

/**
 * An extension of `DisplayVideoResource` to represent an advertiser.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers
 * @final
 */
class Advertiser extends DisplayVideoResource {
  /**
   * Constructs an instance of `Advertiser`.
   *
   * @param {{
   *     id: ?string,
   *     displayName: string,
   *     partnerId: string,
   *     generalConfig: !AdvertiserGeneralConfig,
   * }} requiredParams
   * @param {{
   *     adServerConfig: (!AdvertiserAdServerConfig|undefined),
   *     status: (!Status|undefined),
   * }=} optionalParams
   */
  constructor({
        id,
        displayName,
        partnerId,
        generalConfig,
      }, {
        adServerConfig = {thirdPartyOnlyConfig: {}},
        status = Status.ACTIVE
      } = {}) {
    super(id, displayName, status);

    /** @private @const {string} */
    this.partnerId_ = partnerId;

    /** @private @const {!AdvertiserGeneralConfig} */
    this.generalConfig_ = generalConfig;

    /** @private @const {!AdvertiserAdServerConfig} */
    this.adServerConfig_ = adServerConfig;
  }

  /**
   * Converts a resource object returned by the API into a concrete `Advertiser`
   * instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!Advertiser} The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static fromApiResource(resource) {
    const properties = [
      'advertiserId',
      'displayName',
      'partnerId',
      'entityStatus',
      'generalConfig',
      'adServerConfig',
    ];
    if (ObjectUtil.hasOwnProperties(resource, properties)) {
      const generalConfig = resource['generalConfig'];
      const adServerConfig = resource['adServerConfig'];
      const mappedGeneralConfig =
          AdvertiserGeneralConfigMapper.map(generalConfig);
      const mappedAdServerConfig =
          AdvertiserAdServerConfigMapper.map(adServerConfig);

      if (mappedGeneralConfig && mappedAdServerConfig) {
        return new Advertiser({
          id: String(resource['advertiserId']),
          displayName: String(resource['displayName']),
          partnerId: String(resource['partnerId']),
          generalConfig: mappedGeneralConfig,
        }, {
          adServerConfig: mappedAdServerConfig,
          status: StatusMapper.map(String(resource['entityStatus'])),
        });
      }
    }
    throw new Error(
        'Error! Encountered an invalid API resource object ' +
        'while mapping to an instance of Advertiser.');
  }

  /**
   * Converts this instance of `Advertiser` to its expected JSON representation.
   * This method is called by default when an instance of `Advertiser` is passed
   * to `JSON.stringify`.
   *
   * @return {!Object<string, *>} The custom JSON representation of this
   *     `Advertiser` instance
   */
  toJSON() {
    return {
      advertiserId: this.getId(),
      displayName: this.getDisplayName(),
      partnerId: this.getPartnerId(),
      entityStatus: String(this.getStatus()),
      generalConfig: this.getGeneralConfig(),
      adServerConfig: this.getAdServerConfig(),
    };
  }

  /**
   * Compares this `Advertiser` to 'other' and returns an `Array` of changed
   * mutable properties (ID for example is immutable and cannot be changed,
   * therefore this method will not compare it between 'this' and 'other').
   * @see #getMutableProperties for a complete list of mutable properties.
   *
   * @param {?DisplayVideoResource} other The other advertiser to compare
   * @return {!Array<string>} An array of changed mutable properties between
   *     this and 'other'
   * @override
   */
  getChangedProperties(other) {
    const changedProperties = super.getChangedProperties(other);

    if (other instanceof Advertiser &&
        this.getGeneralConfig().domainUrl !==
            other.getGeneralConfig().domainUrl) {
      changedProperties.push('generalConfig.domainUrl');
    }
    return changedProperties;
  }

  /**
   * Returns all properties of this `Advertiser` that are modifiable.
   *
   * @return {!Array<string>} An array of properties that are modifiable
   * @override
   */
  getMutableProperties() {
    return [...super.getMutableProperties(), 'generalConfig.domainUrl'];
  }

  /**
   * Returns the partner ID.
   *
   * @return {string}
   */
  getPartnerId() {
    return this.partnerId_;
  }

  /**
   * Returns the advertiser general config.
   *
   * @return {!AdvertiserGeneralConfig}
   */
  getGeneralConfig() {
    return this.generalConfig_;
  }

  /**
   * Sets the domain URL property of the advertiser general config.
   *
   * @param {string} domainUrl
   */
  setDomainUrl(domainUrl) {
    this.getGeneralConfig().domainUrl = domainUrl;
  }

  /**
   * Returns the advertiser ad server config.
   *
   * @return {!AdvertiserAdServerConfig}
   */
  getAdServerConfig() {
    return this.adServerConfig_;
  }
}

/**
 * An extension of `DisplayVideoResource` to represent a campaign.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.campaigns
 * @final
 */
class Campaign extends DisplayVideoResource {
  /**
   * Constructs an instance of `Campaign`.
   *
   * @param {{
   *     id: ?string,
   *     displayName: string,
   *     advertiserId: string,
   *     campaignBudgets: !Array<!CampaignBudget>,
   *     campaignGoal: !CampaignGoal,
   *     frequencyCap: !FrequencyCap,
   * }} requiredParams
   * @param {{
   *     campaignFlight: (!CampaignFlight|undefined),
   *     status: (!Status|undefined),
   * }=} optionalParams
   */
  constructor(
      {
        id,
        displayName,
        advertiserId,
        campaignGoal,
        campaignBudgets,
        frequencyCap,
      },
      {
        campaignFlight = {plannedDates: {startDate: ApiDate.now()}},
        status = Status.ACTIVE,
      } = {}) {
    super(id, displayName, status);

    /** @private @const {string} */
    this.advertiserId_ = advertiserId;

    /** @private @const {!CampaignGoal} */
    this.campaignGoal_ = campaignGoal;

    /** @private @const {!FrequencyCap} */
    this.campaignFrequencyCap_ = frequencyCap;

    /** @private @const {!CampaignFlight} */
    this.campaignFlight_ = campaignFlight;

    /** @private @const { !Array<!CampaignBudget>} */
    this.campaignBudgets_ = campaignBudgets;
  }

  /**
   * Converts a resource object returned by the API into a concrete `Campaign`
   * instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!Campaign} The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static fromApiResource(resource) {
    const properties = [
      'campaignId',
      'displayName',
      'advertiserId',
      'entityStatus',
      'campaignBudgets',
      'campaignGoal',
      'campaignFlight',
      'frequencyCap',
    ];
    if (ObjectUtil.hasOwnProperties(resource, properties)) {
      const campaignBudgets = resource['campaignBudgets'];
      const campaignGoal = resource['campaignGoal'];
      const campaignFlight = resource['campaignFlight'];
      const frequencyCap = resource['frequencyCap'];
      const mappedCampaignBudgets = CampaignBudgetMapper.map(campaignBudgets);
      const mappedCampaignGoal = CampaignGoalMapper.map(campaignGoal);
      const mappedCampaignFlight = CampaignFlightMapper.map(campaignFlight);
      const mappedFrequencyCap = FrequencyCapMapper.map(frequencyCap);

      if (mappedCampaignBudgets && mappedCampaignGoal && mappedCampaignFlight &&
          mappedFrequencyCap) {
        return new Campaign(
            {
              id: String(resource['campaignId']),
              displayName: String(resource['displayName']),
              advertiserId: String(resource['advertiserId']),
              campaignBudgets: mappedCampaignBudgets,
              campaignGoal: mappedCampaignGoal,
              frequencyCap: mappedFrequencyCap,
            },
            {
              campaignFlight: mappedCampaignFlight,
              status: StatusMapper.map(String(resource['entityStatus'])),
            });
      }
    }
    throw new Error(
        'Error! Encountered an invalid API resource object ' +
        'while mapping to an instance of Campaign.');
  }

  /**
   * Converts this instance of `Campaign` to its expected JSON representation.
   * This method is called by default when an instance of `Campaign` is passed
   * to `JSON.stringify`.
   *
   * @return {!Object<string, *>} The custom JSON representation of this
   *     `Campaign` instance
   */
  toJSON() {
    return {
      campaignId: this.getId(),
      displayName: this.getDisplayName(),
      advertiserId: this.getAdvertiserId(),
      entityStatus: String(this.getStatus()),
      campaignBudgets: CampaignBudgetMapper.toJson(this.getCampaignBudgets()),
      campaignGoal: this.getCampaignGoal(),
      campaignFlight: CampaignFlightMapper.toJson(this.getCampaignFlight()),
      frequencyCap: this.getCampaignFrequencyCap(),
    };
  }

  /**
   * Compares this `Campaign` to 'other' and returns an `Array` of changed
   * mutable properties (ID for example is immutable and cannot be changed,
   * therefore this method will not compare it between 'this' and 'other').
   * @see #getMutableProperties for a complete list of mutable properties.
   *
   * @param {?DisplayVideoResource} other The other campaign to compare
   * @return {!Array<string>} An array of changed mutable properties between
   *     this and 'other'
   * @override
   */
  getChangedProperties(other) {
    const changedProperties = super.getChangedProperties(other);

    if (other instanceof Campaign) {
      changedProperties.push(
          ...this.getCampaignStartDate().getChangedProperties(
              other.getCampaignStartDate(),
              /* prefix= */ 'campaignFlight.plannedDates.startDate.'));
    }
    return changedProperties;
  }

  /**
   * Returns all properties of this `Campaign` that are modifiable.
   *
   * @return {!Array<string>} An array of properties that are modifiable
   * @override
   */
  getMutableProperties() {
    return [
      ...super.getMutableProperties(),
      ...ApiDate.getMutableProperties('campaignFlight.plannedDates.startDate.'),
    ];
  }

  /**
   * Returns the advertiser ID.
   *
   * @return {string}
   */
  getAdvertiserId() {
    return this.advertiserId_;
  }

  /**
   * Returns the campaign goal configuration.
   *
   * @return {!CampaignGoal}
   */
  getCampaignGoal() {
    return this.campaignGoal_;
  }

  /**
   * Returns the campaign budget
   *
   * @return { !Array<!CampaignBudget>}
   */
  getCampaignBudgets() {
    return this.campaignBudgets_;
  }

  /**
   * Returns the campaign flight configuration.
   *
   * @return {!CampaignFlight}
   */
  getCampaignFlight() {
    return this.campaignFlight_;
  }

  /**
   * Returns the campaign start date.
   *
   * @return {!ApiDate}
   */
  getCampaignStartDate() {
    return this.getCampaignFlight().plannedDates.startDate;
  }

  /**
   * Sets the campaign start date.
   *
   * @param {!ApiDate} campaignStartDate
   */
  setCampaignStartDate(campaignStartDate) {
    this.getCampaignFlight().plannedDates.startDate = campaignStartDate;
  }

  /**
   * Returns the campaign frequency cap configuration.
   *
   * @return {!FrequencyCap}
   */
  getCampaignFrequencyCap() {
    return this.campaignFrequencyCap_;
  }
}

/**
 * An extension of `DisplayVideoResource` to represent an insertion order.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.insertionOrders
 * @final
 */
class InsertionOrder extends DisplayVideoResource {
  /**
   * Constructs an instance of `InsertionOrder`.
   *
   * @param {{
   *     id: ?string,
   *     displayName: string,
   *     advertiserId: string,
   *     campaignId: string,
   *     insertionOrderType: string,
   *     pacing: !Pacing,
   *     frequencyCap: !FrequencyCap,
   *     performanceGoal: !PerformanceGoal,
   *     budget: !InsertionOrderBudget,
   * }} params
   * @param {!Status=} status Optional status to set
   */
  constructor({
        id,
        displayName,
        advertiserId,
        campaignId,
        insertionOrderType,
        pacing,
        frequencyCap,
        performanceGoal,
        budget,
      }, status = Status.DRAFT) {
    super(id, displayName, status);

    /** @private @const {string} */
    this.advertiserId_ = advertiserId;

    /** @private @const {string} */
    this.campaignId_ = campaignId;

    /** @private {string} */
    this.insertionOrderType_ = insertionOrderType;

    /** @private @const {!Pacing} */
    this.insertionOrderPacing_ = pacing;

    /** @private @const {!FrequencyCap} */
    this.insertionOrderFrequencyCap_ = frequencyCap;

    /** @private @const {!PerformanceGoal} */
    this.insertionOrderPerformanceGoal_ = performanceGoal;

    /** @private @const {!InsertionOrderBudget} */
    this.insertionOrderBudget_ = budget;
  }

  /**
   * Converts a resource object returned by the API into a concrete
   * `InsertionOrder` instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!InsertionOrder} The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static fromApiResource(resource) {
    const properties = [
      'insertionOrderId',
      'displayName',
      'advertiserId',
      'campaignId',
      'insertionOrderType',
      'entityStatus',
      'pacing',
      'frequencyCap',
      'performanceGoal',
      'budget',
    ];
    if (ObjectUtil.hasOwnProperties(resource, properties)) {
      const pacing = resource['pacing'];
      const frequencyCap = resource['frequencyCap'];
      const performanceGoal = resource['performanceGoal'];
      const budget = resource['budget'];
      const mappedPacing = PacingMapper.map(pacing);
      const mappedFrequencyCap = FrequencyCapMapper.map(frequencyCap);
      const mappedPerformanceGoal = PerformanceGoalMapper.map(performanceGoal);
      const mappedBudget = InsertionOrderBudgetMapper.map(budget);

      if (mappedPacing && mappedFrequencyCap && mappedPerformanceGoal &&
          mappedBudget) {
        return new InsertionOrder({
          id: String(resource['insertionOrderId']),
          displayName: String(resource['displayName']),
          advertiserId: String(resource['advertiserId']),
          campaignId: String(resource['campaignId']),
          insertionOrderType: String(resource['insertionOrderType']),
          pacing: mappedPacing,
          frequencyCap: mappedFrequencyCap,
          performanceGoal: mappedPerformanceGoal,
          budget: mappedBudget,
        }, StatusMapper.map(String(resource['entityStatus'])));
      }
    }
    throw new Error(
        'Error! Encountered an invalid API resource object ' +
        'while mapping to an instance of InsertionOrder.');
  }

  /**
   * Converts this instance of `InsertionOrder` to its expected JSON
   * representation. This method is called by default when an instance of
   * `InsertionOrder` gets passed to `JSON.stringify`.
   *
   * @return {!Object<string, *>} The custom JSON representation of this
   *     `InsertionOrder` instance
   */
  toJSON() {
    return {
      insertionOrderId: this.getId(),
      displayName: this.getDisplayName(),
      advertiserId: this.getAdvertiserId(),
      campaignId: this.getCampaignId(),
      insertionOrderType: this.getInsertionOrderType(),
      entityStatus: String(this.getStatus()),
      pacing: this.getInsertionOrderPacing(),
      frequencyCap: this.getInsertionOrderFrequencyCap(),
      performanceGoal: this.getInsertionOrderPerformanceGoal(),
      budget: InsertionOrderBudgetMapper.toJson(this.getInsertionOrderBudget()),
    };
  }

  /**
   * Compares this `InsertionOrder` to 'other' and returns an `Array` of
   * changed mutable properties (ID for example is immutable and cannot be
   * changed, therefore this method will not compare it between 'this' and
   * 'other').
   * @see #getMutableProperties for a complete list of mutable properties.
   *
   * @param {?DisplayVideoResource} other The other insertion order to compare
   * @return {!Array<string>} An array of changed mutable properties between
   *     this and 'other'
   * @override
   */
  getChangedProperties(other) {
    const changedProperties = super.getChangedProperties(other);

    if (other instanceof InsertionOrder) {
      if (this.getInsertionOrderType() !== other.getInsertionOrderType()) {
        changedProperties.push('insertionOrderType');
      }
      if (this.getInsertionOrderBudgetSegments() !==
          other.getInsertionOrderBudgetSegments()) {
        changedProperties.push('budget.budgetSegments');
      }
    }
    return changedProperties;
  }

  /**
   * Returns all properties of this `InsertionOrder` that are modifiable.
   *
   * @return {!Array<string>} An array of properties that are modifiable
   * @override
   */
  getMutableProperties() {
    return [
      ...super.getMutableProperties(),
      'insertionOrderType',
      'budget.budgetSegments',
    ];
  }

  /**
   * Returns the advertiser ID.
   *
   * @return {string}
   */
  getAdvertiserId() {
    return this.advertiserId_;
  }

  /**
   * Returns the campaign ID.
   *
   * @return {string}
   */
  getCampaignId() {
    return this.campaignId_;
  }

  /**
   * Returns the insertion order type.
   *
   * @return {string}
   */
  getInsertionOrderType() {
    return this.insertionOrderType_;
  }

  /**
   * Sets the insertion order type.
   *
   * @param {string} insertionOrderType
   */
  setInsertionOrderType(insertionOrderType) {
    this.insertionOrderType_ = insertionOrderType;
  }

  /**
   * Returns the insertion order pacing configuration.
   *
   * @return {!Pacing}
   */
  getInsertionOrderPacing() {
    return this.insertionOrderPacing_;
  }

  /**
   * Returns the insertion order frequency cap configuration.
   *
   * @return {!FrequencyCap}
   */
  getInsertionOrderFrequencyCap() {
    return this.insertionOrderFrequencyCap_;
  }

  /**
   * Returns the insertion order performance goal configuration.
   *
   * @return {!PerformanceGoal}
   */
  getInsertionOrderPerformanceGoal() {
    return this.insertionOrderPerformanceGoal_;
  }

  /**
   * Returns the insertion order budget configuration.
   *
   * @return {!InsertionOrderBudget}
   */
  getInsertionOrderBudget() {
    return this.insertionOrderBudget_;
  }

  /**
   * Returns the insertion order budget segments array.
   *
   * @return {!Array<!InsertionOrderBudgetSegment>}
   */
  getInsertionOrderBudgetSegments() {
    return this.getInsertionOrderBudget().budgetSegments;
  }

  /**
   * Sets the insertion order budget segments array.
   *
   * @param {!Array<!InsertionOrderBudgetSegment>} insertionOrderBudgetSegments
   */
  setInsertionOrderBudgetSegments(insertionOrderBudgetSegments) {
    this.getInsertionOrderBudget().budgetSegments =
        insertionOrderBudgetSegments;
  }
}

/**
 * An extension of `DisplayVideoResource` to represent a line item.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.lineItems
 * @final
 */
class LineItem extends DisplayVideoResource {
  /**
   * Constructs an instance of `LineItem`.
   *
   * @param {{
   *     id: ?string,
   *     displayName: string,
   *     advertiserId: string,
   *     campaignId: string,
   *     insertionOrderId: string,
   *     lineItemType: string,
   *     flight: !LineItemFlight,
   *     budget: !LineItemBudget,
   *     pacing: !Pacing,
   *     frequencyCap: !FrequencyCap,
   *     partnerRevenueModel: !LineItemPartnerRevenueModel,
   *     bidStrategy: !BiddingStrategy,
   * }} params
   * @param {!Status=} status Optional status to set
   */
  constructor({
        id,
        displayName,
        advertiserId,
        campaignId,
        insertionOrderId,
        lineItemType,
        flight,
        budget,
        pacing,
        frequencyCap,
        partnerRevenueModel,
        bidStrategy,
      }, status = Status.DRAFT) {
    super(id, displayName, status);

    /** @private @const {string} */
    this.advertiserId_ = advertiserId;

    /** @private @const {string} */
    this.campaignId_ = campaignId;

    /** @private @const {string} */
    this.insertionOrderId_ = insertionOrderId;

    /** @private @const {string} */
    this.lineItemType_ = lineItemType;

    /** @private @const {!LineItemFlight} */
    this.lineItemFlight_ = flight;

    /** @private @const {!LineItemBudget} */
    this.lineItemBudget_ = budget;

    /** @private @const {!Pacing} */
    this.lineItemPacing_ = pacing;

    /** @private @const {!FrequencyCap} */
    this.lineItemFrequencyCap_ = frequencyCap;

    /** @private @const {!LineItemPartnerRevenueModel} */
    this.lineItemPartnerRevenueModel_ = partnerRevenueModel;

    /** @private @const {!BiddingStrategy} */
    this.lineItemBidStrategy_ = bidStrategy;
  }

  /**
   * Converts a resource object returned by the API into a concrete `LineItem`
   * instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!LineItem} The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static fromApiResource(resource) {
    const properties = [
      'lineItemId',
      'displayName',
      'advertiserId',
      'campaignId',
      'insertionOrderId',
      'lineItemType',
      'entityStatus',
      'flight',
      'budget',
      'pacing',
      'frequencyCap',
      'partnerRevenueModel',
      'bidStrategy',
    ];
    if (ObjectUtil.hasOwnProperties(resource, properties)) {
      const flight = resource['flight'];
      const budget = resource['budget'];
      const pacing = resource['pacing'];
      const frequencyCap = resource['frequencyCap'];
      const partnerRevenueModel = resource['partnerRevenueModel'];
      const bidStrategy = resource['bidStrategy'];
      const mappedFlight = LineItemFlightMapper.map(flight);
      const mappedBudget = LineItemBudgetMapper.map(budget);
      const mappedPacing = PacingMapper.map(pacing);
      const mappedFrequencyCap = FrequencyCapMapper.map(frequencyCap);
      const mappedPartnerRevenueModel =
          LineItemPartnerRevenueModelMapper.map(partnerRevenueModel);
      const mappedBidStrategy = BiddingStrategyMapper.map(bidStrategy);

      if (mappedFlight && mappedBudget && mappedPacing && mappedFrequencyCap &&
          mappedPartnerRevenueModel && mappedBidStrategy) {
        return new LineItem({
          id: String(resource['lineItemId']),
          displayName: String(resource['displayName']),
          advertiserId: String(resource['advertiserId']),
          campaignId: String(resource['campaignId']),
          insertionOrderId: String(resource['insertionOrderId']),
          lineItemType: String(resource['lineItemType']),
          flight: mappedFlight,
          budget: mappedBudget,
          pacing: mappedPacing,
          frequencyCap: mappedFrequencyCap,
          partnerRevenueModel: mappedPartnerRevenueModel,
          bidStrategy: mappedBidStrategy,
        }, StatusMapper.map(String(resource['entityStatus'])));
      }
    }
    throw new Error(
        'Error! Encountered an invalid API resource object ' +
        'while mapping to an instance of LineItem.');
  }

  /**
   * Converts this instance of `LineItem` to its expected JSON representation.
   * This method is called by default when an instance of `LineItem` gets passed
   * to `JSON.stringify`.
   *
   * @return {!Object<string, *>} The custom JSON representation of this
   *     `LineItem` instance
   */
  toJSON() {
    return {
      lineItemId: this.getId(),
      displayName: this.getDisplayName(),
      advertiserId: this.getAdvertiserId(),
      campaignId: this.getCampaignId(),
      insertionOrderId: this.getInsertionOrderId(),
      lineItemType: this.getLineItemType(),
      entityStatus: String(this.getStatus()),
      flight: LineItemFlightMapper.toJson(this.getLineItemFlight()),
      budget: this.getLineItemBudget(),
      pacing: this.getLineItemPacing(),
      frequencyCap: this.getLineItemFrequencyCap(),
      partnerRevenueModel: this.getLineItemPartnerRevenueModel(),
      bidStrategy: this.getLineItemBidStrategy(),
    };
  }

  /**
   * Compares this `LineItem` to 'other' and returns an `Array` of changed
   * mutable properties (ID for example is immutable and cannot be changed,
   * therefore this method will not compare it between 'this' and 'other').
   * @see #getMutableProperties for a complete list of mutable properties.
   *
   * @param {?DisplayVideoResource} other The other line item to compare
   * @return {!Array<string>} An array of changed mutable properties between
   *     this and 'other'
   * @override
   */
  getChangedProperties(other) {
    const changedProperties = super.getChangedProperties(other);

    if (other instanceof LineItem && this.getLineItemFlightEndDate()) {
      changedProperties.push(
          ...this.getLineItemFlightEndDate().getChangedProperties(
              other.getLineItemFlightEndDate(),
              /* prefix= */ 'flight.dateRange.endDate.'));
    }
    return changedProperties;
  }

  /**
   * Returns all properties of this `LineItem` that are modifiable.
   *
   * @return {!Array<string>} An array of properties that are modifiable
   * @override
   */
  getMutableProperties() {
    return [
      ...super.getMutableProperties(),
      ...ApiDate.getMutableProperties('flight.dateRange.endDate.'),
    ];
  }

  /**
   * Returns the advertiser ID.
   *
   * @return {string}
   */
  getAdvertiserId() {
    return this.advertiserId_;
  }

  /**
   * Returns the campaign ID.
   *
   * @return {string}
   */
  getCampaignId() {
    return this.campaignId_;
  }

  /**
   * Returns the insertion order ID.
   *
   * @return {string}
   */
  getInsertionOrderId() {
    return this.insertionOrderId_;
  }

  /**
   * Returns the line item type.
   *
   * @return {string}
   */
  getLineItemType() {
    return this.lineItemType_;
  }

  /**
   * Returns the line item flight configuration.
   *
   * @return {!LineItemFlight}
   */
  getLineItemFlight() {
    return this.lineItemFlight_;
  }

  /**
   * Returns the line item flight end date, or null if a date object doesn't
   * exist.
   *
   * @return {?ApiDate}
   */
  getLineItemFlightEndDate() {
    return this.getLineItemFlight().dateRange ?
        this.getLineItemFlight().dateRange.endDate : null;
  }

  /**
   * Sets the line item flight end date, only if a date object exists.
   *
   * @param {!ApiDate} lineItemFlightEndDate
   */
  setLineItemFlightEndDate(lineItemFlightEndDate) {
    if (this.getLineItemFlight().dateRange) {
      this.getLineItemFlight().dateRange.endDate = lineItemFlightEndDate;
    }
  }

  /**
   * Returns the line item budget configuration.
   *
   * @return {!LineItemBudget}
   */
  getLineItemBudget() {
    return this.lineItemBudget_;
  }

  /**
   * Returns the line item pacing configuration.
   *
   * @return {!Pacing}
   */
  getLineItemPacing() {
    return this.lineItemPacing_;
  }

  /**
   * Returns the line item frequency cap configuration.
   *
   * @return {!FrequencyCap}
   */
  getLineItemFrequencyCap() {
    return this.lineItemFrequencyCap_;
  }

  /**
   * Returns the line item partner revenue model configuration.
   *
   * @return {!LineItemPartnerRevenueModel}
   */
  getLineItemPartnerRevenueModel() {
    return this.lineItemPartnerRevenueModel_;
  }

  /**
   * Returns the line item bid strategy configuration.
   *
   * @return {!BiddingStrategy}
   */
  getLineItemBidStrategy() {
    return this.lineItemBidStrategy_;
  }
}

/**
 * An extension of `DisplayVideoResource` to represent an inventory source.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/inventorySources
 * @final
 */
class InventorySource extends DisplayVideoResource {
  /**
   * Constructs an instance of `InventorySource`.
   *
   * @param {{
   *     id: string,
   *     displayName: string,
   *     inventorySourceType: string,
   *     rateDetails: !InventorySourceRateDetails,
   * }} requiredParams
   * @param {{
   *     commitment: (?string|undefined),
   *     deliveryMethod: (?string|undefined),
   *     dealId: (?string|undefined),
   *     publisherName: (?string|undefined),
   *     exchange: (?string|undefined),
   *     status: (!Status|undefined),
   * }=} optionalParams
   */
  constructor({
        id,
        displayName,
        inventorySourceType,
        rateDetails,
      }, {
        commitment = null,
        deliveryMethod = null,
        dealId = null,
        publisherName = null,
        exchange = null,
        status = Status.ACTIVE,
      } = {}) {
    super(id, displayName, status);

    /** @private @const {string} */
    this.inventorySourceType_ = inventorySourceType;

    /** @private @const {!InventorySourceRateDetails} */
    this.rateDetails_ = rateDetails;

    /** @private @const {?string} */
    this.commitment_ = commitment;

    /** @private @const {?string} */
    this.deliveryMethod_ = deliveryMethod;

    /** @private @const {?string} */
    this.dealId_ = dealId;

    /** @private @const {?string} */
    this.publisherName_ = publisherName;

    /** @private @const {?string} */
    this.exchange_ = exchange;
  }

  /**
   * Converts a resource object returned by the API into a concrete
   * `InventorySource` instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!InventorySource} The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static fromApiResource(resource) {
    const properties = [
      'inventorySourceId',
      'displayName',
      'inventorySourceType',
      'rateDetails',
      'status',
    ];
    if (ObjectUtil.hasOwnProperties(resource, properties)) {
      const status = resource['status'];
      const rateDetails = resource['rateDetails'];
      const mappedRateDetails =
          InventorySourceRateDetailsMapper.map(rateDetails);

      if (mappedRateDetails &&
          ObjectUtil.hasOwnProperties(status, ['entityStatus'])) {
        const requiredParams = {
          id: String(resource['inventorySourceId']),
          displayName: String(resource['displayName']),
          inventorySourceType: String(resource['inventorySourceType']),
          rateDetails: mappedRateDetails,
        };
        const optionalParams = {
          status: StatusMapper.map(String(status['entityStatus'])),
        };
        if (resource['commitment']) {
          optionalParams['commitment'] = resource['commitment'];
        }
        if (resource['deliveryMethod']) {
          optionalParams['deliveryMethod'] = resource['deliveryMethod'];
        }
        if (resource['dealId']) {
          optionalParams['dealId'] = resource['dealId'];
        }
        if (resource['publisherName']) {
          optionalParams['publisherName'] = resource['publisherName'];
        }
        if (resource['exchange']) {
          optionalParams['exchange'] = resource['exchange'];
        }
        return new InventorySource(requiredParams, optionalParams);
      }
    }
    throw new Error(
        'Error! Encountered an invalid API resource object ' +
        'while mapping to an instance of InventorySource.');
  }

  /**
   * Converts this instance of `InventorySource` to its expected JSON
   * representation. This method is called by default when an instance of
   * `InventorySource` gets passed to `JSON.stringify`.
   *
   * @return {!Object<string, *>} The custom
   *     JSON representation of this `InventorySource` instance
   */
  toJSON() {
    const result = {
      inventorySourceId: this.getId(),
      displayName: this.getDisplayName(),
      inventorySourceType: this.getInventorySourceType(),
      rateDetails: this.getRateDetails(),
      status: {
        entityStatus: String(this.getStatus()),
      },
    };
    if (this.getCommitment()) {
      result['commitment'] = this.getCommitment();
    }
    if (this.getDeliveryMethod()) {
      result['deliveryMethod'] = this.getDeliveryMethod();
    }
    if (this.getDealId()) {
      result['dealId'] = this.getDealId();
    }
    if (this.getPublisherName()) {
      result['publisherName'] = this.getPublisherName();
    }
    if (this.getExchange()) {
      result['exchange'] = this.getExchange();
    }
    return result;
  }

  /**
   * @param {?DisplayVideoResource} other
   * @return {!Array<string>}
   * @override
   */
  getChangedProperties(other) {
    return [];
  }

  /**
   * @return {!Array<string>}
   * @override
   */
  getMutableProperties() {
    return [];
  }

  /**
   * Returns the inventory source type.
   *
   * @return {string}
   */
  getInventorySourceType() {
    return this.inventorySourceType_;
  }

  /**
   * Returns the rate details.
   *
   * @return {!InventorySourceRateDetails}
   */
  getRateDetails() {
    return this.rateDetails_;
  }

  /**
   * Returns the commitment.
   *
   * @return {?string}
   */
  getCommitment() {
    return this.commitment_;
  }

  /**
   * Returns the delivery method.
   *
   * @return {?string}
   */
  getDeliveryMethod() {
    return this.deliveryMethod_;
  }

  /**
   * Returns the deal ID.
   *
   * @return {?string}
   */
  getDealId() {
    return this.dealId_;
  }

  /**
   * Returns the publisher name.
   *
   * @return {?string}
   */
  getPublisherName() {
    return this.publisherName_;
  }

  /**
   * Returns the exchange.
   *
   * @return {?string}
   */
  getExchange() {
    return this.exchange_;
  }
}

/**
 * An extension of `DisplayVideoResource` to represent a targeting option.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/targetingTypes.targetingOptions
 */
class TargetingOption extends DisplayVideoResource {
  /**
   * Constructs an instance of `TargetingOption`.
   *
   * @param {?string} id The unique resource ID
   * @param {!TargetingType} targetingType The targeting type for this targeting
   *     option
   * @param {string} targetingDetailsKey The property name for the targeting
   *     details object associated with this targeting option
   * @param {!Object<string, *>} targetingDetails The targeting details
   *     object, which may contain a 'displayName' property
   * @param {string=} idProperty Optional name of the ID property. Defaults to
   *     'targetingOptionId'
   */
  constructor(
      id, targetingType, targetingDetailsKey, targetingDetails,
      idProperty = 'targetingOptionId') {
    super(
        id,
        /* displayName= */
        targetingDetails['displayName'] ?
            String(targetingDetails['displayName']) :
            null);

    /** @private @const {!TargetingType} */
    this.targetingType_ = targetingType;

    /** @private @const {string} */
    this.targetingDetailsKey_ = targetingDetailsKey;

    /** @private @const {!Object<string, string>} */
    this.targetingDetails_ = targetingDetails;

    /** @private @const {string} */
    this.idProperty_ = idProperty;
  }

  /**
   * Converts a resource object returned by the API into a concrete
   * `TargetingOption` instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @param {!Array<string>=} additionalProperties Optional additional
   *     properties. Defaults to an empty array
   * @param {string=} idProperty Optional id property to use. Defaults to
   *     'targetingOptionId'
   * @param {string=} type Optional type to use for logging. Defaults to
   *     'TargetingOption'
   * @return {!TargetingOption} The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static fromApiResource(
      resource, additionalProperties = [], idProperty = 'targetingOptionId',
      type = 'TargetingOption') {
    const properties = [
      'targetingType',
      idProperty,
      ...additionalProperties,
    ];

    if (ObjectUtil.hasOwnProperties(resource, properties)) {
      const keys = Object.keys(resource).filter(
          (key) => ![...properties, 'name'].includes(key));

      if (keys.length === 1) {
        const targetingDetailsKey = keys[0];
        const targetingDetails = resource[targetingDetailsKey];

        if (ObjectUtil.isObject(targetingDetails)) {
          return new TargetingOption(
              String(resource[idProperty]),
              TargetingTypeMapper.map(String(resource['targetingType'])),
              targetingDetailsKey,
              /** @type {!Object<string, *>} */ (targetingDetails));
        }
      }
    }
    throw new Error(
        'Error! Encountered an invalid API resource object ' +
        `while mapping to an instance of ${type}.`);
  }

  /**
   * Converts this instance of `TargetingOption` to its expected JSON
   * representation. This method is called by default when an instance of
   * `TargetingOption` gets passed to `JSON.stringify`.
   *
   * @return {!Object<string, *>} The custom JSON representation of this
   *     `TargetingOption` instance
   */
  toJSON() {
    const result = {
      targetingType: this.getTargetingType(),
    };
    result[this.getTargetingDetailsKey()] = this.getTargetingDetails();
    result[this.getIdProperty()] = this.getId();

    return result;
  }

  /**
   * @param {?DisplayVideoResource} other
   * @return {!Array<string>}
   * @override
   */
  getChangedProperties(other) {
    return [];
  }

  /**
   * @return {!Array<string>}
   * @override
   */
  getMutableProperties() {
    return [];
  }

  /**
   * Returns the targeting type.
   *
   * @return {!TargetingType}
   */
  getTargetingType() {
    return this.targetingType_;
  }

  /**
   * Returns the targeting details key.
   *
   * @return {string}
   */
  getTargetingDetailsKey() {
    return this.targetingDetailsKey_;
  }

  /**
   * Returns the targeting details object.
   *
   * @return {!Object<string, *>}
   */
  getTargetingDetails() {
    return this.targetingDetails_;
  }

  /**
   * Returns the id property.
   *
   * @return {string}
   */
  getIdProperty() {
    return this.idProperty_;
  }
}

/**
 * An extension of `DisplayVideoResource` to represent an assigned targeting
 * option. It is either assigned to an `Advertiser`, `Campaign`,
 * `InsertionOrder` or `LineItem`.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.targetingTypes.assignedTargetingOptions
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.campaigns.targetingTypes.assignedTargetingOptions
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.insertionOrders.targetingTypes.assignedTargetingOptions
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.lineItems.targetingTypes.assignedTargetingOptions
 * @final
 */
class AssignedTargetingOption extends TargetingOption {
  /**
   * Constructs an instance of `AssignedTargetingOption`.
   *
   * @param {?string} id The unique resource ID
   * @param {!TargetingType} targetingType The targeting type for this targeting
   *     option
   * @param {string} inheritance Indicates whether the assigned taregting option
   *     is inherited from a higher level entity
   * @param {string} targetingDetailsKey The property name for the assigned
   *     targeting details object associated with this targeting option
   * @param {!Object<string, *>} targetingDetails The targeting details object
   *     which may contain a 'displayName' property
   */
  constructor(
      id, targetingType, inheritance, targetingDetailsKey, targetingDetails) {
    super(
        id, targetingType, targetingDetailsKey, targetingDetails,
        'assignedTargetingOptionId');

    /** @private @const {string} */
    this.inheritance_ = inheritance;
  }

  /**
   * Converts a resource object returned by the API into a concrete
   * `AssignedTargetingOption` instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!AssignedTargetingOption} The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static fromApiResource(resource) {
    const targetingOption = TargetingOption.fromApiResource(
        resource,
        /* additionalProperties= */ ['inheritance'],
        /* idProperty= */ 'assignedTargetingOptionId',
        /* type= */ 'AssignedTargetingOption');
    return new AssignedTargetingOption(
        /** @type {string} */ (targetingOption.getId()),
        targetingOption.getTargetingType(),
        String(resource['inheritance']),
        targetingOption.getTargetingDetailsKey(),
        targetingOption.getTargetingDetails());
  }

  /**
   * Converts this instance of `AssignedTargetingOption` to its expected JSON
   * representation. This method is called by default when an instance of
   * `AssignedTargetingOption` gets passed to `JSON.stringify`.
   *
   * @return {!Object<string, *>} The custom JSON representation of this
   *     `AssignedTargetingOption` instance
   * @override
   */
  toJSON() {
    const result = super.toJSON();
    result['inheritance'] = this.getInheritance();

    return result;
  }

  /**
   * Returns the inheritance.
   *
   * @return {string}
   */
  getInheritance() {
    return this.inheritance_;
  }
}
