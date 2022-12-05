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

import {
  AdvertiserAdServerConfig,
  AdvertiserAdServerConfigMapper,
  AdvertiserGeneralConfig,
  AdvertiserGeneralConfigMapper,
  ApiDate,
  BiddingStrategy,
  BiddingStrategyMapper,
  CampaignBudget,
  CampaignBudgetMapper,
  CampaignFlight,
  CampaignFlightMapper,
  CampaignGoal,
  CampaignGoalMapper,
  FrequencyCap,
  FrequencyCapMapper,
  InsertionOrderBudget,
  InsertionOrderBudgetMapper,
  InsertionOrderBudgetSegment,
  InventorySourceRateDetails,
  InventorySourceRateDetailsMapper,
  LineItemBudget,
  LineItemBudgetMapper,
  LineItemFlight,
  LineItemFlightMapper,
  LineItemPartnerRevenueModel,
  LineItemPartnerRevenueModelMapper,
  Pacing,
  PacingMapper,
  PerformanceGoal,
  PerformanceGoalMapper,
  RawApiDate,
  RawStatus,
  RawTargetingType,
  Status,
  STATUS,
  StatusMapper,
  TargetingType,
  TargetingTypeMapper,
} from './dv360_types';
import {ObjectUtil} from './utils';

/** A base class for DV360 resources that are accessible via the API. */
export class DisplayVideoResource {

  /**
   * Constructs an instance of `DisplayVideoResource`.
   *
   * @param id The unique resource ID. Should be null for resources
   *     that are yet to be created by the API
   * @param displayName The display name. Can be null for certain
   *     resources
   * @param status Optional status to set
   */
  constructor(
    private readonly id: string | null,
    private displayName: string | null,
    private status: Status = STATUS.UNSPECIFIED
  ) {
  }

  /**
   * Compares this `DisplayVideoResource` to 'other' and returns an `Array` of
   * changed mutable properties (ID for example is immutable and cannot be
   * changed (it can only be "set" after an object has been created by the API),
   * therefore this method will not compare it between 'this' and 'other').
   * @see #getMutableProperties for a complete list of mutable properties.
   *
   * @param other The other resource to compare
   * @return An array of changed mutable properties between
   *     this and 'other'
   */
  getChangedProperties(other: DisplayVideoResource | null): string[] {
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
   * @param other The other resource to compare
   * @return A comma-separated string of changed mutable properties
   *     between this and 'other'
   */
  getChangedPropertiesString(other: DisplayVideoResource | null): string {
    return this.getChangedProperties(other).join(',');
  }

  /**
   * Returns all properties of this `DisplayVideoResource` that are modifiable.
   *
   * @return An array of properties that are modifiable
   */
  getMutableProperties(): string[] {
    return ['displayName', 'entityStatus'];
  }

  /**
   * Returns the API resource ID.
   *
   */
  getId(): string | null {
    return this.id;
  }

  /**
   * Returns the API resource display name.
   *
   */
  getDisplayName(): string | null {
    return this.displayName;
  }

  /**
   * Sets the API resource display name.
   *
   */
  setDisplayName(displayName: string) {
    this.displayName = displayName;
  }

  /**
   * Returns the API resource status.
   *
   */
  getStatus(): Status {
    return this.status;
  }

  /**
   * Sets the API resource status.
   *
   */
  setStatus(status: Status) {
    this.status = status;
  }
}

interface RequiredAdvertiserParams {
  id: string | null;
  displayName: string;
  partnerId: string;
  generalConfig: AdvertiserGeneralConfig;
}

interface OptionalAdvertiserParams {
  adServerConfig?: AdvertiserAdServerConfig;
  status?: Status;
}

/**
 * An extension of `DisplayVideoResource` to represent an advertiser.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers
 * @final
 */
export class Advertiser extends DisplayVideoResource {
  private readonly partnerId: string;

  private readonly generalConfig: AdvertiserGeneralConfig;

  private readonly adServerConfig: AdvertiserAdServerConfig;

  /**
   * Constructs an instance of `Advertiser`.
   *
   */
  constructor(
    {id, displayName, partnerId, generalConfig}: RequiredAdvertiserParams,
    {
      adServerConfig = {thirdPartyOnlyConfig: {}},
      status = STATUS.ACTIVE,
    }: OptionalAdvertiserParams = {}
  ) {
    super(id, displayName, status);

    this.partnerId = partnerId;

    this.generalConfig = generalConfig;

    this.adServerConfig = adServerConfig;
  }

  /**
   * Converts a resource object returned by the API into a concrete `Advertiser`
   * instance.
   *
   * @param resource The API resource object
   * @return The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static fromApiResource(resource: {[key: string]: unknown}): Advertiser {
    const properties = [
      'advertiserId',
      'displayName',
      'partnerId',
      'entityStatus',
      'generalConfig',
      'adServerConfig',
    ];
    if (ObjectUtil.hasOwnProperties(resource, properties)) {
      const generalConfig = resource[
        'generalConfig'
      ] as AdvertiserGeneralConfig;
      const adServerConfig = resource[
        'adServerConfig'
      ] as AdvertiserAdServerConfig;
      const mappedGeneralConfig =
        AdvertiserGeneralConfigMapper.map(generalConfig);
      const mappedAdServerConfig =
        AdvertiserAdServerConfigMapper.map(adServerConfig);

      if (mappedGeneralConfig && mappedAdServerConfig) {
        return new Advertiser(
          {
            id: String(resource['advertiserId']),
            displayName: String(resource['displayName']),
            partnerId: String(resource['partnerId']),
            generalConfig: mappedGeneralConfig,
          },
          {
            adServerConfig: mappedAdServerConfig ?? null,
            status: StatusMapper.map(resource['entityStatus'] as RawStatus),
          }
        );
      }
    }
    throw new Error(
      'Error! Encountered an invalid API resource object ' +
        'while mapping to an instance of Advertiser.'
    );
  }

  /**
   * Converts this instance of `Advertiser` to its expected JSON representation.
   * This method is called by default when an instance of `Advertiser` is passed
   * to `JSON.stringify`.
   *
   * @return The custom JSON representation of this
   *     `Advertiser` instance
   */
  toJSON(): {[key: string]: unknown} {
    return {
      'advertiserId': this.getId(),
      'displayName': this.getDisplayName(),
      'partnerId': this.getPartnerId(),
      'entityStatus': String(this.getStatus()),
      'generalConfig': this.getGeneralConfig(),
      'adServerConfig': this.getAdServerConfig(),
    };
  }

  /**
   * Compares this `Advertiser` to 'other' and returns an `Array` of changed
   * mutable properties (ID for example is immutable and cannot be changed,
   * therefore this method will not compare it between 'this' and 'other').
   * @see #getMutableProperties for a complete list of mutable properties.
   *
   * @param other The other advertiser to compare
   * @return An array of changed mutable properties between
   *     this and 'other'
   */
  override getChangedProperties(other: DisplayVideoResource | null): string[] {
    const changedProperties = super.getChangedProperties(other);

    if (
      other instanceof Advertiser &&
      this.getGeneralConfig().domainUrl !== other.getGeneralConfig().domainUrl
    ) {
      changedProperties.push('generalConfig.domainUrl');
    }
    return changedProperties;
  }

  /**
   * Returns all properties of this `Advertiser` that are modifiable.
   *
   * @return An array of properties that are modifiable
   */
  override getMutableProperties(): string[] {
    return [...super.getMutableProperties(), 'generalConfig.domainUrl'];
  }

  /**
   * Returns the partner ID.
   *
   */
  getPartnerId(): string {
    return this.partnerId;
  }

  /**
   * Returns the advertiser general config.
   *
   */
  getGeneralConfig(): AdvertiserGeneralConfig {
    return this.generalConfig;
  }

  /**
   * Sets the domain URL property of the advertiser general config.
   *
   */
  setDomainUrl(domainUrl: string) {
    this.getGeneralConfig().domainUrl = domainUrl;
  }

  /**
   * Returns the advertiser ad server config.
   *
   */
  getAdServerConfig(): AdvertiserAdServerConfig {
    return this.adServerConfig;
  }
}

interface CampaignRequiredParameters {
  id: string | null;
  displayName: string;
  advertiserId: string;
  campaignGoal: CampaignGoal;
  frequencyCap: FrequencyCap;
}

interface CampaignOptionalParameters {
  campaignBudgets?: CampaignBudget[];
  campaignFlight?: CampaignFlight;
  status?: Status;
}

/**
 * An extension of `DisplayVideoResource` to represent a campaign.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.campaigns
 * @final
 */
export class Campaign extends DisplayVideoResource {
  private readonly advertiserId: string;

  private readonly campaignGoal: CampaignGoal;

  private readonly campaignFrequencyCap: FrequencyCap;

  private readonly campaignFlight: CampaignFlight;

  private readonly campaignBudgets: CampaignBudget[];
  /**
   * Constructs an instance of `Campaign`.
   *
   */
  constructor(
    {
      id,
      displayName,
      advertiserId,
      campaignGoal,
      frequencyCap,
    }: CampaignRequiredParameters,
    {
      campaignBudgets,
      campaignFlight = {plannedDates: {startDate: ApiDate.now().toJSON()}},
      status = STATUS.ACTIVE,
    }: CampaignOptionalParameters = {}
  ) {
    super(id, displayName, status);

    this.advertiserId = advertiserId;

    this.campaignGoal = campaignGoal;

    this.campaignFrequencyCap = frequencyCap;

    this.campaignFlight = campaignFlight;

    this.campaignBudgets = campaignBudgets || [];
  }

  /**
   * Converts a resource object returned by the API into a concrete `Campaign`
   * instance.
   *
   * @param resource The API resource object
   * @return The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static fromApiResource(resource: {[key: string]: unknown}): Campaign {
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
      const campaignBudgets = resource['campaignBudgets'] as CampaignBudget[];
      const campaignGoal = resource['campaignGoal'] as CampaignGoal;
      const campaignFlight = resource['campaignFlight'] as CampaignFlight;
      const frequencyCap = resource['frequencyCap'] as FrequencyCap;
      const mappedCampaignBudgets = CampaignBudgetMapper.map(campaignBudgets);
      const mappedCampaignGoal = CampaignGoalMapper.map(campaignGoal);
      const mappedCampaignFlight = CampaignFlightMapper.map(campaignFlight);
      const mappedFrequencyCap = FrequencyCapMapper.map(frequencyCap);

      if (mappedCampaignGoal && mappedCampaignFlight && mappedFrequencyCap) {
        return new Campaign(
          {
            id: String(resource['campaignId']),
            displayName: String(resource['displayName']),
            advertiserId: String(resource['advertiserId']),
            campaignGoal: mappedCampaignGoal,
            frequencyCap: mappedFrequencyCap,
          },
          {
            campaignBudgets: mappedCampaignBudgets,
            campaignFlight: mappedCampaignFlight,
            status: StatusMapper.map(
              String(resource['entityStatus']) as RawStatus
            ),
          }
        );
      }
    }
    throw new Error(
      'Error! Encountered an invalid API resource object ' +
        'while mapping to an instance of Campaign.'
    );
  }

  /**
   * Converts this instance of `Campaign` to its expected JSON representation.
   * This method is called by default when an instance of `Campaign` is passed
   * to `JSON.stringify`.
   *
   * @return The custom JSON representation of this
   *     `Campaign` instance
   */
  toJSON(): {[key: string]: unknown} {
    return {
      'campaignId': this.getId(),
      'displayName': this.getDisplayName(),
      'advertiserId': this.getAdvertiserId(),
      'entityStatus': String(this.getStatus()),
      'campaignBudgets': CampaignBudgetMapper.toJson(this.getCampaignBudgets()),
      'campaignGoal': this.getCampaignGoal(),
      'campaignFlight': CampaignFlightMapper.toJson(this.getCampaignFlight()),
      'frequencyCap': this.getCampaignFrequencyCap(),
    };
  }

  /**
   * Compares this `Campaign` to 'other' and returns an `Array` of changed
   * mutable properties (ID for example is immutable and cannot be changed,
   * therefore this method will not compare it between 'this' and 'other').
   * @see #getMutableProperties for a complete list of mutable properties.
   *
   * @param other The other campaign to compare
   * @return An array of changed mutable properties between
   *     this and 'other'
   */
  override getChangedProperties(other: DisplayVideoResource | null): string[] {
    const changedProperties = super.getChangedProperties(other);

    if (other instanceof Campaign) {
      changedProperties.push(
        ...ApiDate.fromApiResource(
          this.getCampaignStartDate()
        )!.getChangedProperties(
          other.getCampaignStartDate(),
          /* prefix= */ 'campaignFlight.plannedDates.startDate.'
        )
      );
    }
    return changedProperties;
  }

  /**
   * Returns all properties of this `Campaign` that are modifiable.
   *
   * @return An array of properties that are modifiable
   */
  override getMutableProperties(): string[] {
    return [
      ...super.getMutableProperties(),
      ...ApiDate.getMutableProperties('campaignFlight.plannedDates.startDate.'),
    ];
  }

  /**
   * Returns the advertiser ID.
   *
   */
  getAdvertiserId(): string {
    return this.advertiserId;
  }

  /**
   * Returns the campaign goal configuration.
   *
   */
  getCampaignGoal(): CampaignGoal {
    return this.campaignGoal;
  }

  /**
   * Returns the campaign budget
   *
   */
  getCampaignBudgets(): CampaignBudget[] {
    return this.campaignBudgets;
  }

  /**
   * Returns the campaign flight configuration.
   *
   */
  getCampaignFlight(): CampaignFlight {
    return this.campaignFlight;
  }

  /**
   * Returns the campaign start date.
   *
   */
  getCampaignStartDate(): RawApiDate {
    return this.getCampaignFlight().plannedDates.startDate;
  }

  /**
   * Sets the campaign start date.
   *
   */
  setCampaignStartDate(campaignStartDate: RawApiDate) {
    this.getCampaignFlight().plannedDates.startDate = campaignStartDate;
  }

  /**
   * Returns the campaign frequency cap configuration.
   *
   */
  getCampaignFrequencyCap(): FrequencyCap {
    return this.campaignFrequencyCap;
  }
}

interface InsertionOrderParams {
  id: string | null;
  displayName: string;
  advertiserId: string;
  campaignId: string;
  insertionOrderType: string;
  pacing: Pacing;
  frequencyCap: FrequencyCap;
  performanceGoal: PerformanceGoal;
  budget: InsertionOrderBudget;
}

/**
 * An extension of `DisplayVideoResource` to represent an insertion order.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.insertionOrders
 * @final
 */
export class InsertionOrder extends DisplayVideoResource {
  private readonly advertiserId: string;

  private readonly campaignId: string;

  private insertionOrderType: string;

  private readonly insertionOrderPacing: Pacing;

  private readonly insertionOrderFrequencyCap: FrequencyCap;

  private readonly insertionOrderPerformanceGoal: PerformanceGoal;

  private readonly insertionOrderBudget: InsertionOrderBudget;

  constructor(
    {
      id,
      displayName,
      advertiserId,
      campaignId,
      insertionOrderType,
      pacing,
      frequencyCap,
      performanceGoal,
      budget,
    }: InsertionOrderParams,
    status: Status = STATUS.DRAFT
  ) {
    super(id, displayName, status);

    this.advertiserId = advertiserId;

    this.campaignId = campaignId;

    this.insertionOrderType = insertionOrderType;

    this.insertionOrderPacing = pacing;

    this.insertionOrderFrequencyCap = frequencyCap;

    this.insertionOrderPerformanceGoal = performanceGoal;

    this.insertionOrderBudget = budget;
  }

  /**
   * Converts a resource object returned by the API into a concrete
   * `InsertionOrder` instance.
   *
   * @param resource The API resource object
   * @return The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static fromApiResource(resource: {[key: string]: unknown}): InsertionOrder {
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
      const pacing = resource['pacing'] as Pacing;
      const frequencyCap = resource['frequencyCap'] as FrequencyCap;
      const performanceGoal = resource['performanceGoal'] as PerformanceGoal;
      const budget = resource['budget'] as InsertionOrderBudget;
      const mappedPacing = PacingMapper.map(pacing);
      const mappedFrequencyCap = FrequencyCapMapper.map(frequencyCap);
      const mappedPerformanceGoal = PerformanceGoalMapper.map(performanceGoal);
      const mappedBudget = InsertionOrderBudgetMapper.map(budget);

      if (
        mappedPacing &&
        mappedFrequencyCap &&
        mappedPerformanceGoal &&
        mappedBudget
      ) {
        return new InsertionOrder(
          {
            id: String(resource['insertionOrderId']),
            displayName: String(resource['displayName']),
            advertiserId: String(resource['advertiserId']),
            campaignId: String(resource['campaignId']),
            insertionOrderType: String(resource['insertionOrderType']),
            pacing: mappedPacing,
            frequencyCap: mappedFrequencyCap,
            performanceGoal: mappedPerformanceGoal,
            budget: mappedBudget,
          },
          StatusMapper.map(resource['entityStatus'] as RawStatus)
        );
      }
    }
    throw new Error(
      'Error! Encountered an invalid API resource object ' +
        'while mapping to an instance of InsertionOrder.'
    );
  }

  /**
   * Converts this instance of `InsertionOrder` to its expected JSON
   * representation. This method is called by default when an instance of
   * `InsertionOrder` gets passed to `JSON.stringify`.
   *
   * @return The custom JSON representation of this
   *     `InsertionOrder` instance
   */
  toJSON(): {[key: string]: unknown} {
    return {
      'insertionOrderId': this.getId(),
      'displayName': this.getDisplayName(),
      'advertiserId': this.getAdvertiserId(),
      'campaignId': this.getCampaignId(),
      'insertionOrderType': this.getInsertionOrderType(),
      'entityStatus': String(this.getStatus()),
      'pacing': this.getInsertionOrderPacing(),
      'frequencyCap': this.getInsertionOrderFrequencyCap(),
      'performanceGoal': this.getInsertionOrderPerformanceGoal(),
      'budget': InsertionOrderBudgetMapper.toJson(
        this.getInsertionOrderBudget()
      ),
    };
  }

  /**
   * Compares this `InsertionOrder` to 'other' and returns an `Array` of
   * changed mutable properties (ID for example is immutable and cannot be
   * changed, therefore this method will not compare it between 'this' and
   * 'other').
   * @see #getMutableProperties for a complete list of mutable properties.
   *
   * @param other The other insertion order to compare
   * @return An array of changed mutable properties between
   *     this and 'other'
   */
  override getChangedProperties(other: DisplayVideoResource | null): string[] {
    const changedProperties = super.getChangedProperties(other);

    if (other instanceof InsertionOrder) {
      if (this.getInsertionOrderType() !== other.getInsertionOrderType()) {
        changedProperties.push('insertionOrderType');
      }
      if (
        this.getInsertionOrderBudgetSegments() !==
        other.getInsertionOrderBudgetSegments()
      ) {
        changedProperties.push('budget.budgetSegments');
      }
    }
    return changedProperties;
  }

  /**
   * Returns all properties of this `InsertionOrder` that are modifiable.
   *
   * @return An array of properties that are modifiable
   */
  override getMutableProperties(): string[] {
    return [
      ...super.getMutableProperties(),
      'insertionOrderType',
      'budget.budgetSegments',
    ];
  }

  /**
   * Returns the advertiser ID.
   *
   */
  getAdvertiserId(): string {
    return this.advertiserId;
  }

  /**
   * Returns the campaign ID.
   *
   */
  getCampaignId(): string {
    return this.campaignId;
  }

  /**
   * Returns the insertion order type.
   *
   */
  getInsertionOrderType(): string {
    return this.insertionOrderType;
  }

  /**
   * Sets the insertion order type.
   *
   */
  setInsertionOrderType(insertionOrderType: string) {
    this.insertionOrderType = insertionOrderType;
  }

  /**
   * Returns the insertion order pacing configuration.
   *
   */
  getInsertionOrderPacing(): Pacing {
    return this.insertionOrderPacing;
  }

  /**
   * Returns the insertion order frequency cap configuration.
   *
   */
  getInsertionOrderFrequencyCap(): FrequencyCap {
    return this.insertionOrderFrequencyCap;
  }

  /**
   * Returns the insertion order performance goal configuration.
   *
   */
  getInsertionOrderPerformanceGoal(): PerformanceGoal {
    return this.insertionOrderPerformanceGoal;
  }

  /**
   * Returns the insertion order budget configuration.
   *
   */
  getInsertionOrderBudget(): InsertionOrderBudget {
    return this.insertionOrderBudget;
  }

  /**
   * Returns the insertion order budget segments array.
   *
   */
  getInsertionOrderBudgetSegments(): InsertionOrderBudgetSegment[] {
    return this.getInsertionOrderBudget().budgetSegments;
  }

  /**
   * Sets the insertion order budget segments array.
   *
   */
  setInsertionOrderBudgetSegments(
    insertionOrderBudgetSegments: InsertionOrderBudgetSegment[]
  ) {
    this.getInsertionOrderBudget().budgetSegments =
      insertionOrderBudgetSegments;
  }
}

interface LineItemParams {
  id: string | null;
  displayName: string;
  advertiserId: string;
  campaignId: string;
  insertionOrderId: string;
  lineItemType: string;
  flight: LineItemFlight;
  budget: LineItemBudget;
  pacing: Pacing;
  frequencyCap: FrequencyCap;
  partnerRevenueModel: LineItemPartnerRevenueModel;
  bidStrategy: BiddingStrategy;
}

/**
 * An extension of `DisplayVideoResource` to represent a line item.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/advertisers.lineItems
 * @final
 */
export class LineItem extends DisplayVideoResource {
  private readonly advertiserId: string;

  private readonly campaignId: string;

  private readonly insertionOrderId: string;

  private readonly lineItemType: string;

  private readonly lineItemFlight: LineItemFlight;

  private readonly lineItemBudget: LineItemBudget;

  private readonly lineItemPacing: Pacing;

  private readonly lineItemFrequencyCap: FrequencyCap;

  private readonly lineItemPartnerRevenueModel: LineItemPartnerRevenueModel;

  private readonly lineItemBidStrategy: BiddingStrategy;

  constructor(
    {
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
    }: LineItemParams,
    status: Status = STATUS.DRAFT
  ) {
    super(id, displayName, status);

    this.advertiserId = advertiserId;

    this.campaignId = campaignId;

    this.insertionOrderId = insertionOrderId;

    this.lineItemType = lineItemType;

    this.lineItemFlight = flight;

    this.lineItemBudget = budget;

    this.lineItemPacing = pacing;

    this.lineItemFrequencyCap = frequencyCap;

    this.lineItemPartnerRevenueModel = partnerRevenueModel;

    this.lineItemBidStrategy = bidStrategy;
  }

  /**
   * Converts a resource object returned by the API into a concrete `LineItem`
   * instance.
   *
   * @param resource The API resource object
   * @return The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static fromApiResource(resource: {[key: string]: unknown}): LineItem {
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
      const flight = resource['flight'] as LineItemFlight;
      const budget = resource['budget'] as LineItemBudget;
      const pacing = resource['pacing'] as Pacing;
      const frequencyCap = resource['frequencyCap'] as FrequencyCap;
      const partnerRevenueModel = resource[
        'partnerRevenueModel'
      ] as LineItemPartnerRevenueModel;
      const bidStrategy = resource['bidStrategy'] as BiddingStrategy;
      const mappedFlight = LineItemFlightMapper.map(flight);
      const mappedBudget = LineItemBudgetMapper.map(budget);
      const mappedPacing = PacingMapper.map(pacing);
      const mappedFrequencyCap = FrequencyCapMapper.map(frequencyCap);
      const mappedPartnerRevenueModel =
        LineItemPartnerRevenueModelMapper.map(partnerRevenueModel);
      const mappedBidStrategy = BiddingStrategyMapper.map(bidStrategy);

      if (
        mappedFlight &&
        mappedBudget &&
        mappedPacing &&
        mappedFrequencyCap &&
        mappedPartnerRevenueModel &&
        mappedBidStrategy
      ) {
        return new LineItem(
          {
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
          },
          StatusMapper.map(resource['entityStatus'] as RawStatus)
        );
      }
    }
    throw new Error(
      'Error! Encountered an invalid API resource object ' +
        'while mapping to an instance of LineItem.'
    );
  }

  /**
   * Converts this instance of `LineItem` to its expected JSON representation.
   * This method is called by default when an instance of `LineItem` gets passed
   * to `JSON.stringify`.
   *
   * @return The custom JSON representation of this
   *     `LineItem` instance
   */
  toJSON(): {[key: string]: unknown} {
    return {
      'lineItemId': this.getId(),
      'displayName': this.getDisplayName(),
      'advertiserId': this.getAdvertiserId(),
      'campaignId': this.getCampaignId(),
      'insertionOrderId': this.getInsertionOrderId(),
      'lineItemType': this.getLineItemType(),
      'entityStatus': String(this.getStatus()),
      'flight': LineItemFlightMapper.toJson(this.getLineItemFlight()),
      'budget': this.getLineItemBudget(),
      'pacing': this.getLineItemPacing(),
      'frequencyCap': this.getLineItemFrequencyCap(),
      'partnerRevenueModel': this.getLineItemPartnerRevenueModel(),
      'bidStrategy': this.getLineItemBidStrategy(),
    };
  }

  /**
   * Compares this `LineItem` to 'other' and returns an `Array` of changed
   * mutable properties (ID for example is immutable and cannot be changed,
   * therefore this method will not compare it between 'this' and 'other').
   * @see #getMutableProperties for a complete list of mutable properties.
   *
   * @param other The other line item to compare
   * @return An array of changed mutable properties between
   *     this and 'other'
   */
  override getChangedProperties(other: DisplayVideoResource | null): string[] {
    const changedProperties = super.getChangedProperties(other);

    if (other instanceof LineItem && this.getLineItemFlightEndDate()) {
      changedProperties.push(
        ...ApiDate.fromApiResource(
          this.getLineItemFlightEndDate()!
        )!.getChangedProperties(
          other.getLineItemFlightEndDate(),
          /* prefix= */ 'flight.dateRange.endDate.'
        )
      );
    }
    return changedProperties;
  }

  /**
   * Returns all properties of this `LineItem` that are modifiable.
   *
   * @return An array of properties that are modifiable
   */
  override getMutableProperties(): string[] {
    return [
      ...super.getMutableProperties(),
      ...ApiDate.getMutableProperties('flight.dateRange.endDate.'),
    ];
  }

  /**
   * Returns the advertiser ID.
   *
   */
  getAdvertiserId(): string {
    return this.advertiserId;
  }

  /**
   * Returns the campaign ID.
   *
   */
  getCampaignId(): string {
    return this.campaignId;
  }

  /**
   * Returns the insertion order ID.
   *
   */
  getInsertionOrderId(): string {
    return this.insertionOrderId;
  }

  /**
   * Returns the line item type.
   *
   */
  getLineItemType(): string {
    return this.lineItemType;
  }

  /**
   * Returns the line item flight configuration.
   *
   */
  getLineItemFlight(): LineItemFlight {
    return this.lineItemFlight;
  }

  /**
   * Returns the line item flight end date, or null if a date object doesn't
   * exist.
   *
   */
  getLineItemFlightEndDate(): RawApiDate | null {
    return this.getLineItemFlight().dateRange
      ? this.getLineItemFlight().dateRange!.endDate
      : null;
  }

  /**
   * Sets the line item flight end date, only if a date object exists.
   *
   */
  setLineItemFlightEndDate(lineItemFlightEndDate: RawApiDate) {
    if (this.getLineItemFlight().dateRange) {
      this.getLineItemFlight().dateRange!.endDate = lineItemFlightEndDate;
    }
  }

  /**
   * Returns the line item budget configuration.
   *
   */
  getLineItemBudget(): LineItemBudget {
    return this.lineItemBudget;
  }

  /**
   * Returns the line item pacing configuration.
   *
   */
  getLineItemPacing(): Pacing {
    return this.lineItemPacing;
  }

  /**
   * Returns the line item frequency cap configuration.
   *
   */
  getLineItemFrequencyCap(): FrequencyCap {
    return this.lineItemFrequencyCap;
  }

  /**
   * Returns the line item partner revenue model configuration.
   *
   */
  getLineItemPartnerRevenueModel(): LineItemPartnerRevenueModel {
    return this.lineItemPartnerRevenueModel;
  }

  /**
   * Returns the line item bid strategy configuration.
   *
   */
  getLineItemBidStrategy(): BiddingStrategy {
    return this.lineItemBidStrategy;
  }
}

interface InventorySourceParams {
  id: string;
  displayName: string;
  inventorySourceType: string;
  rateDetails: InventorySourceRateDetails;
}

interface InventorySourceOptionalParams {
  commitment?: string | null;
  deliveryMethod?: string | null;
  dealId?: string | null;
  publisherName?: string | null;
  exchange?: string | null;
  status?: Status;
}

/**
 * An extension of `DisplayVideoResource` to represent an inventory source.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/inventorySources
 * @final
 */
export class InventorySource extends DisplayVideoResource {
  private readonly inventorySourceType: string;

  private readonly rateDetails: InventorySourceRateDetails;

  private readonly commitment: string | null;

  private readonly deliveryMethod: string | null;

  private readonly dealId: string | null;

  private readonly publisherName: string | null;

  private readonly exchange: string | null;

  constructor(
    {id, displayName, inventorySourceType, rateDetails}: InventorySourceParams,
    {
      commitment = null,
      deliveryMethod = null,
      dealId = null,
      publisherName = null,
      exchange = null,
      status = STATUS.ACTIVE,
    }: InventorySourceOptionalParams = {}
  ) {
    super(id, displayName, status);

    this.inventorySourceType = inventorySourceType;

    this.rateDetails = rateDetails;

    this.commitment = commitment;

    this.deliveryMethod = deliveryMethod;

    this.dealId = dealId;

    this.publisherName = publisherName;

    this.exchange = exchange;
  }

  /**
   * Converts a resource object returned by the API into a concrete
   * `InventorySource` instance.
   *
   * @param resource The API resource object
   * @return The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static fromApiResource(resource: {[key: string]: unknown}): InventorySource {
    const properties = [
      'inventorySourceId',
      'displayName',
      'inventorySourceType',
      'rateDetails',
      'status',
    ];
    if (ObjectUtil.hasOwnProperties(resource, properties)) {
      const status = resource['status'] as {entityStatus?: RawStatus};
      const rateDetails = resource['rateDetails'] as InventorySourceRateDetails;
      const mappedRateDetails =
        InventorySourceRateDetailsMapper.map(rateDetails);

      if (
        mappedRateDetails &&
        ObjectUtil.hasOwnProperties(status, ['entityStatus'])
      ) {
        const requiredParams = {
          id: String(resource['inventorySourceId']),
          displayName: String(resource['displayName']),
          inventorySourceType: String(resource['inventorySourceType']),
          rateDetails: mappedRateDetails,
        };
        const optionalParams: InventorySourceOptionalParams = {
          status: StatusMapper.map(status.entityStatus!),
        };
        if (resource['commitment']) {
          optionalParams.commitment = resource['commitment'] as string;
        }
        if (resource['deliveryMethod']) {
          optionalParams.deliveryMethod = resource['deliveryMethod'] as string;
        }
        if (resource['dealId']) {
          optionalParams.dealId = resource['dealId'] as string;
        }
        if (resource['publisherName']) {
          optionalParams.publisherName = resource['publisherName'] as string;
        }
        if (resource['exchange']) {
          optionalParams.exchange = resource['exchange'] as string;
        }
        return new InventorySource(requiredParams, optionalParams);
      }
    }
    throw new Error(
      'Error! Encountered an invalid API resource object ' +
        'while mapping to an instance of InventorySource.'
    );
  }

  /**
   * Converts this instance of `InventorySource` to its expected JSON
   * representation. This method is called by default when an instance of
   * `InventorySource` gets passed to `JSON.stringify`.
   *
   * @return The custom
   *     JSON representation of this `InventorySource` instance
   */
  toJSON(): {[key: string]: unknown} {
    const result: {[key: string]: unknown} = {
      'inventorySourceId': this.getId(),
      'displayName': this.getDisplayName(),
      'inventorySourceType': this.getInventorySourceType(),
      'rateDetails': this.getRateDetails(),
      'status': {entityStatus: String(this.getStatus())},
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

  override getChangedProperties(other: DisplayVideoResource | null): string[] {
    return [];
  }

  override getMutableProperties(): string[] {
    return [];
  }

  /**
   * Returns the inventory source type.
   *
   */
  getInventorySourceType(): string {
    return this.inventorySourceType;
  }

  /**
   * Returns the rate details.
   *
   */
  getRateDetails(): InventorySourceRateDetails {
    return this.rateDetails;
  }

  /**
   * Returns the commitment.
   *
   */
  getCommitment(): string | null {
    return this.commitment;
  }

  /**
   * Returns the delivery method.
   *
   */
  getDeliveryMethod(): string | null {
    return this.deliveryMethod;
  }

  /**
   * Returns the deal ID.
   *
   */
  getDealId(): string | null {
    return this.dealId;
  }

  /**
   * Returns the publisher name.
   *
   */
  getPublisherName(): string | null {
    return this.publisherName;
  }

  /**
   * Returns the exchange.
   *
   */
  getExchange(): string | null {
    return this.exchange;
  }
}

/**
 * An extension of `DisplayVideoResource` to represent a targeting option.
 * @see https://developers.google.com/display-video/api/reference/rest/v1/targetingTypes.targetingOptions
 */
export class TargetingOption extends DisplayVideoResource {

  private readonly targetingDetails: {[key: string]: unknown};
  /**
   * Constructs an instance of `TargetingOption`.
   *
   * @param id The unique resource ID
   * @param targetingType The targeting type for this targeting
   *     option
   * @param targetingDetailsKey The property name for the targeting
   *     details object associated with this targeting option
   * @param targetingDetails The targeting details
   *     object, which may contain a 'displayName' property
   * @param idProperty Optional name of the ID property. Defaults to
   *     'targetingOptionId'
   */
  constructor(
    id: string | null,
    private readonly targetingType: TargetingType,
    private readonly targetingDetailsKey: string,
    targetingDetails: {[key: string]: unknown},
    private readonly idProperty: string = 'targetingOptionId'
  ) {
    super(
      id /* displayName= */,
      targetingDetails['displayName']
        ? String(targetingDetails['displayName'])
        : null
    );

    this.targetingDetails = targetingDetails;
  }

  /**
   * Converts a resource object returned by the API into a concrete
   * `TargetingOption` instance.
   *
   * @param resource The API resource object
   * @param additionalProperties Optional additional
   *     properties. Defaults to an empty array
   * @param idProperty Optional id property to use. Defaults to
   *     'targetingOptionId'
   * @param type Optional type to use for logging. Defaults to
   *     'TargetingOption'
   * @return The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static fromApiResource(
    resource: {[key: string]: unknown},
    additionalProperties: string[] = [],
    idProperty: string = 'targetingOptionId',
    type: string = 'TargetingOption'
  ): TargetingOption {
    const properties = ['targetingType', idProperty, ...additionalProperties];

    if (ObjectUtil.hasOwnProperties(resource, properties)) {
      const keys = Object.keys(resource).filter(
        (key) => ![...properties, 'name'].includes(key)
      );

      if (keys.length === 1) {
        const targetingDetailsKey = keys[0];
        const targetingDetails = resource[targetingDetailsKey];

        if (ObjectUtil.isObject(targetingDetails)) {
          return new TargetingOption(
            String(resource[idProperty]),
            TargetingTypeMapper.map(
              resource['targetingType'] as RawTargetingType
            )!,
            targetingDetailsKey,
            targetingDetails as {[key: string]: unknown}
          );
        }
      }
    }
    throw new Error(
      'Error! Encountered an invalid API resource object ' +
        `while mapping to an instance of ${type}.`
    );
  }

  /**
   * Converts this instance of `TargetingOption` to its expected JSON
   * representation. This method is called by default when an instance of
   * `TargetingOption` gets passed to `JSON.stringify`.
   *
   * @return The custom JSON representation of this
   *     `TargetingOption` instance
   */
  toJSON(): {[key: string]: unknown} {
    const result: {[key: string]: unknown} = {
      'targetingType': this.getTargetingType(),
    };
    result[this.getTargetingDetailsKey()] = this.getTargetingDetails();
    result[this.getIdProperty()] = this.getId();

    return result;
  }

  override getChangedProperties(other: DisplayVideoResource | null): string[] {
    return [];
  }

  override getMutableProperties(): string[] {
    return [];
  }

  /**
   * Returns the targeting type.
   *
   */
  getTargetingType(): TargetingType {
    return this.targetingType;
  }

  /**
   * Returns the targeting details key.
   *
   */
  getTargetingDetailsKey(): string {
    return this.targetingDetailsKey;
  }

  /**
   * Returns the targeting details object.
   *
   */
  getTargetingDetails(): {[key: string]: unknown} {
    return this.targetingDetails;
  }

  /**
   * Returns the id property.
   *
   */
  getIdProperty(): string {
    return this.idProperty;
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
export class AssignedTargetingOption extends TargetingOption {
  /**
   * Constructs an instance of `AssignedTargetingOption`.
   *
   * @param id The unique resource ID
   * @param targetingType The targeting type for this targeting
   *     option
   * @param inheritance Indicates whether the assigned targeting option
   *     is inherited from a higher level entity
   * @param targetingDetailsKey The property name for the assigned
   *     targeting details object associated with this targeting option
   * @param targetingDetails The targeting details object
   *     which may contain a 'displayName' property
   */
  constructor(
    id: string | null,
    targetingType: TargetingType,
    private readonly inheritance: string,
    targetingDetailsKey: string,
    targetingDetails: {[key: string]: unknown}
  ) {
    super(
      id,
      targetingType,
      targetingDetailsKey,
      targetingDetails,
      'assignedTargetingOptionId'
    );
  }

  /**
   * Converts a resource object returned by the API into a concrete
   * `AssignedTargetingOption` instance.
   *
   * @param resource The API resource object
   * @return The concrete instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  static override fromApiResource(resource: {
    [key: string]: unknown;
  }): AssignedTargetingOption {
    const targetingOption = TargetingOption.fromApiResource(
      resource,
      /* additionalProperties= */ ['inheritance'],
      /* idProperty= */ 'assignedTargetingOptionId',
      /* type= */ 'AssignedTargetingOption'
    );
    return new AssignedTargetingOption(
      targetingOption.getId() as string,
      targetingOption.getTargetingType(),
      String(resource['inheritance']),
      targetingOption.getTargetingDetailsKey(),
      targetingOption.getTargetingDetails()
    );
  }

  /**
   * Converts this instance of `AssignedTargetingOption` to its expected JSON
   * representation. This method is called by default when an instance of
   * `AssignedTargetingOption` gets passed to `JSON.stringify`.
   *
   * @return The custom JSON representation of this
   *     `AssignedTargetingOption` instance
   */
  override toJSON(): {[key: string]: unknown} {
    const result = super.toJSON();
    result['inheritance'] = this.getInheritance();

    return result;
  }

  /**
   * Returns the inheritance.
   *
   */
  getInheritance(): string {
    return this.inheritance;
  }
}
