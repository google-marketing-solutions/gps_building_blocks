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

    if (other && other instanceof Advertiser &&
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
   *     campaignGoal: !CampaignGoal,
   *     frequencyCap: !FrequencyCap,
   * }} requiredParams
   * @param {{
   *     campaignFlight: (!CampaignFlight|undefined),
   *     status: (!Status|undefined),
   * }=} optionalParams
   */
  constructor({
        id,
        displayName,
        advertiserId,
        campaignGoal,
        frequencyCap,
      }, {
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
      'campaignGoal',
      'campaignFlight',
      'frequencyCap',
    ];
    if (ObjectUtil.hasOwnProperties(resource, properties)) {
      const campaignGoal = resource['campaignGoal'];
      const campaignFlight = resource['campaignFlight'];
      const frequencyCap = resource['frequencyCap'];
      const mappedCampaignGoal = CampaignGoalMapper.map(campaignGoal);
      const mappedCampaignFlight = CampaignFlightMapper.map(campaignFlight);
      const mappedFrequencyCap = FrequencyCapMapper.map(frequencyCap);

      if (mappedCampaignGoal && mappedCampaignFlight && mappedFrequencyCap) {
        return new Campaign({
          id: String(resource['campaignId']),
          displayName: String(resource['displayName']),
          advertiserId: String(resource['advertiserId']),
          campaignGoal: mappedCampaignGoal,
          frequencyCap: mappedFrequencyCap,
        }, {
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
      campaignGoal: this.getCampaignGoal(),
      campaignFlight: {
        plannedDates: {startDate: this.getCampaignStartDate().toJSON()},
      },
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

    if (other && other instanceof Campaign) {
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
   * }} params
   * @param {!Status=} status Optional status to set
   */
  constructor({
        id,
        displayName,
        advertiserId,
        campaignId,
        insertionOrderType,
      }, status = Status.DRAFT) {
    super(id, displayName, status);

    /** @private @const {string} */
    this.advertiserId_ = advertiserId;

    /** @private @const {string} */
    this.campaignId_ = campaignId;

    /** @private {string} */
    this.insertionOrderType_ = insertionOrderType;
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
    if (!resource['insertionOrderId'] || !resource['displayName'] ||
        !resource['advertiserId'] || !resource['campaignId'] ||
        !resource['insertionOrderType'] || !resource['entityStatus']) {
      throw new Error(
          'Error! Encountered an invalid API resource object ' +
          'while mapping to an instance of InsertionOrder.');
    }
    return new InsertionOrder({
          id: String(resource['insertionOrderId']),
          displayName: String(resource['displayName']),
          advertiserId: String(resource['advertiserId']),
          campaignId: String(resource['campaignId']),
          insertionOrderType: String(resource['insertionOrderType']),
        }, StatusMapper.map(String(resource['entityStatus'])));
  }

  /**
   * Converts this instance of `InsertionOrder` to its expected JSON
   * representation. This method is called by default when an instance of
   * `InsertionOrder` gets passed to `JSON.stringify`.
   *
   * @return {!Object<string, ?string>} The custom JSON representation of this
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

    if (other && other instanceof InsertionOrder &&
        this.getInsertionOrderType() !== other.getInsertionOrderType()) {
      changedProperties.push('insertionOrderType');
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
    return [...super.getMutableProperties(), 'insertionOrderType'];
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
      flight: this.getLineItemFlight(),
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

    if (other && other instanceof LineItem && this.getLineItemFlightEndDate()) {
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
   *     commitment: string,
   *     deliveryMethod: string,
   *     dealId: string,
   *     publisherName: string,
   *     exchange: string,
   *     rateType: string,
   * }} params
   * @param {!Status=} status Optional status to set
   */
  constructor({
        id,
        displayName,
        inventorySourceType,
        commitment,
        deliveryMethod,
        dealId,
        publisherName,
        exchange,
        rateType,
      }, status = Status.ACTIVE) {
    super(id, displayName, status);

    /** @private @const {string} */
    this.inventorySourceType_ = inventorySourceType;

    /** @private @const {string} */
    this.commitment_ = commitment;

    /** @private @const {string} */
    this.deliveryMethod_ = deliveryMethod;

    /** @private @const {string} */
    this.dealId_ = dealId;

    /** @private @const {string} */
    this.publisherName_ = publisherName;

    /** @private @const {string} */
    this.exchange_ = exchange;

    /** @private @const {string} */
    this.rateType_ = rateType;
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
    if (!resource['inventorySourceId'] || !resource['displayName'] ||
        !resource['inventorySourceType'] || !resource['commitment'] ||
        !resource['deliveryMethod'] || !resource['dealId'] ||
        !resource['publisherName'] || !resource['exchange'] ||
        !resource['rateDetails'] ||
        !resource['rateDetails']['inventorySourceRateType'] ||
        !resource['status'] || !resource['status']['entityStatus']) {
      throw new Error(
          'Error! Encountered an invalid API resource object ' +
          'while mapping to an instance of InventorySource.');
    }
    return new InventorySource({
          id: String(resource['inventorySourceId']),
          displayName: String(resource['displayName']),
          inventorySourceType: String(resource['inventorySourceType']),
          commitment: String(resource['commitment']),
          deliveryMethod: String(resource['deliveryMethod']),
          dealId: String(resource['dealId']),
          publisherName: String(resource['publisherName']),
          exchange: String(resource['exchange']),
          rateType: String(resource['rateDetails']['inventorySourceRateType']),
        }, StatusMapper.map(String(resource['status']['entityStatus'])));
  }

  /**
   * Converts this instance of `InventorySource` to its expected JSON
   * representation. This method is called by default when an instance of
   * `InventorySource` gets passed to `JSON.stringify`.
   *
   * @return {!Object<string, (?string|!Object<string, string>)>} The custom
   *     JSON representation of this `InventorySource` instance
   */
  toJSON() {
    return {
      inventorySourceId: this.getId(),
      displayName: this.getDisplayName(),
      inventorySourceType: this.getInventorySourceType(),
      commitment: this.getCommitment(),
      deliveryMethod: this.getDeliveryMethod(),
      dealId: this.getDealId(),
      publisherName: this.getPublisherName(),
      exchange: this.getExchange(),
      rateDetails: {
        inventorySourceRateType: this.getRateType(),
      },
      status: {
        entityStatus: String(this.getStatus()),
      },
    };
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
   * Returns the commitment.
   *
   * @return {string}
   */
  getCommitment() {
    return this.commitment_;
  }

  /**
   * Returns the delivery method.
   *
   * @return {string}
   */
  getDeliveryMethod() {
    return this.deliveryMethod_;
  }

  /**
   * Returns the deal ID.
   *
   * @return {string}
   */
  getDealId() {
    return this.dealId_;
  }

  /**
   * Returns the publisher name.
   *
   * @return {string}
   */
  getPublisherName() {
    return this.publisherName_;
  }

  /**
   * Returns the exchange.
   *
   * @return {string}
   */
  getExchange() {
    return this.exchange_;
  }

  /**
   * Returns the rate type.
   *
   * @return {string}
   */
  getRateType() {
    return this.rateType_;
  }
}

/**
 * An extension of `DisplayVideoResource` to represent a targeting option.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/targetingTypes.targetingOptions
 * @final
 */
class TargetingOption extends DisplayVideoResource {
  /**
   * Constructs an instance of `TargetingOption`.
   *
   * @param {string} id The unique resource ID
   * @param {!TargetingType} targetingType The targeting type for this targeting
   *     option
   * @param {string} targetingDetailsKey The property name for the targeting
   *     details object associated with this targeting option
   * @param {!Object<string, string>} targetingDetails The targeting details
   *     object, which may contain a 'displayName' property along with a
   *     single string key-value pair representing the targeting details
   */
  constructor(id, targetingType, targetingDetailsKey, targetingDetails) {
    super(id, targetingDetails['displayName'] || null);

    /** @private @const {!TargetingType} */
    this.targetingType_ = targetingType;

    /** @private @const {string} */
    this.targetingDetailsKey_ = targetingDetailsKey;

    /** @private @const {!Object<string, string>} */
    this.targetingDetails_ = targetingDetails;
  }

  /**
   * Converts a resource object returned by the API into a concrete
   * `InventorySource` instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!TargetingOption} The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static fromApiResource(resource) {
    const properties = ['targetingOptionId', 'targetingType'];

    if (ObjectUtil.hasOwnProperties(resource, properties)) {
      const keys = Object.keys(resource).filter(
          (key) => ![...properties, 'name'].includes(key));

      if (keys.length === 1) {
        const targetingDetailsKey = keys[0];
        const targetingDetails = resource[targetingDetailsKey];

        if (ObjectUtil.isObject(targetingDetails)) {
          return new TargetingOption(
              String(resource['targetingOptionId']),
              TargetingTypeMapper.map(String(resource['targetingType'])),
              targetingDetailsKey,
              /** @type {!Object<string, string>} */(targetingDetails));
        }
      }
    }
      throw new Error(
          'Error! Encountered an invalid API resource object ' +
          'while mapping to an instance of TargetingOption.');
    }

  /**
   * Converts this instance of `TargetingOption` to its expected JSON
   * representation. This method is called by default when an instance of
   * `TargetingOption` gets passed to `JSON.stringify`.
   *
   * @return {!Object<string, (?string|!Object<string, string>)>} The custom
   *     JSON representation of this `TargetingOption` instance
   */
  toJSON() {
    const result = {
      targetingOptionId: this.getId(),
      targetingType: this.getTargetingType(),
    };
    result[this.getTargetingDetailsKey()] = this.getTargetingDetails();

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
   * @return {!Object<string, string>}
   */
  getTargetingDetails() {
    return this.targetingDetails_;
  }
}
