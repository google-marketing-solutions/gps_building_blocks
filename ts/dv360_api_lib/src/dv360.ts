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
 * @fileoverview This file encapsulates core functionality for accessing the
 * DV360 API and working with its resources. Class names have been chosen to
 * mimic the 'fluent' approach adopted by typical Google Apps Script client
 * libraries (e.g. 'DisplayVideo.Advertisers.list()').
 */

import {BaseApiClient} from './base';
import {
  Advertiser,
  AssignedTargetingOption,
  Campaign,
  DisplayVideoResource,
  InsertionOrder,
  InventorySource,
  LineItem,
  TargetingOption,
} from './dv360_resources';
import {STATUS, TargetingType, TARGETING_TYPE} from './dv360_types';
import {FilterExpression, Rule, RuleOperator, UriUtil} from './utils';
const API_SCOPE: string = 'displayvideo';

const API_VERSION: string = 'v1';

/**
 * Returns a `FilterExpression` for active entities.
 *
 */
function activeEntityFilter(): FilterExpression {
  return new FilterExpression([
    new Rule('entityStatus', RuleOperator.EQ, STATUS.ACTIVE),
  ]);
}

/**
 * An abstract API client for the DV360 API that extends `BaseApiClient`.
 * Provides base CRUD operations that are used by other resource-specific
 * extension classes to manipulate specific API resources.
 */
export abstract class DisplayVideoApiClient extends BaseApiClient {
  /** Constructs an instance of `DisplayVideoApiClient`. */
  protected constructor(private readonly resourceName: string) {
    super(API_SCOPE, API_VERSION);
  }

  /**
   * Retrieves all resources for the given 'requestUri' from the API, calling
   * 'requestCallback' for every retrieved 'page' of data (i.e. this function
   * has no return value).
   *
   * @param requestUri The URI of the request
   * @param requestCallback The callback to trigger after fetching every 'page' of
   *     results
   * @param maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   * @param requestParams Optional requestParams to
   *     use for the request
   */
  listResources<T extends DisplayVideoResource>(
    requestUri: string,
    requestCallback: (p1: T[]) => void,
    maxPages: number = -1,
    requestParams: {[key: string]: string} = {'method': 'get'}
  ) {
    this.executePagedApiRequest(
      requestUri,
      requestParams,
      /* requestCallback= */ (apiResponse) => {
        const apiResources = apiResponse[this.getResourceName()];

        if (Array.isArray(apiResources)) {
          const resources =
            /** !Array<!Object<string, *>> */
            apiResources.map((resource) =>
              this.asDisplayVideoResource(resource)
            );
          requestCallback(resources as T[]);
        }
      },
      maxPages
    );
  }

  /**
   * Converts a resource object returned by the API into a concrete
   * {@link DisplayVideoResource} instance.
   *
   * @param resource The API resource object
   * @return The concrete instance
   */
  abstract asDisplayVideoResource(resource: {
    [key: string]: unknown;
  }): DisplayVideoResource;

  /**
   * Retrieves a single resource from the API. All required information
   * will already be provided within the given 'requestUri'.
   *
   * @param requestUri The URI of the GET request
   * @return An object representing the retrieved API
   *     resource
   */
  getResource(requestUri: string): DisplayVideoResource {
    return this.asDisplayVideoResource(
      this.executeApiRequest(
        requestUri,
        /* requestParams= */ {'method': 'get'},
        /* retryOnFailure= */ true
      )
    );
  }

  /**
   * Creates an instance of the given API resource, described by 'payload'.
   *
   * @param requestUri The URI of the POST request
   * @param payload The representation of the resource
   *     to create
   * @return The created resource
   */
  createResource(
    requestUri: string,
    payload: DisplayVideoResource
  ): DisplayVideoResource {
    return this.asDisplayVideoResource(
      this.executeApiRequest(
        requestUri,
        /* requestParams= */ {
          'method': 'post',
          'payload': JSON.stringify(payload),
        },
        /* retryOnFailure= */ false
      )
    );
  }

  /**
   * Modifies an API resource. All required information will already be provided
   * within the given 'requestUri'.
   *
   * @param requestUri The URI of the PATCH request
   * @param payload The representation of the resource
   *     to patch
   * @return The updated resource
   */
  patchResource(
    requestUri: string,
    payload: DisplayVideoResource
  ): DisplayVideoResource {
    return this.asDisplayVideoResource(
      this.executeApiRequest(
        requestUri,
        /* requestParams= */ {
          'method': 'patch',
          'payload': JSON.stringify(payload),
        },
        /* retryOnFailure= */ true
      )
    );
  }

  /**
   * Modifies the 'original' resource by identifing properties that have changed
   * after comparing 'original' to its 'modified' counterpart.
   *
   * @param requestUri The URI of the PATCH request
   * @param original The original resource
   * @param modified The modified resource
   * @return An object representing the modified
   *     resource
   */
  patchResourceByComparison(
    requestUri: string,
    original: DisplayVideoResource,
    modified: DisplayVideoResource | null
  ): DisplayVideoResource {
    const changedProperties = original.getChangedPropertiesString(modified);

    return this.patchResource(
      UriUtil.modifyUrlQueryString(
        requestUri,
        'updateMask',
        encodeURIComponent(changedProperties)
      ),
      original
    );
  }

  /**
   * Deletes an instance of the given API resource. All required information
   * will already be provided within the given 'requestUri'.
   *
   * @param requestUri The URI of the DELETE request
   */
  deleteResource(requestUri: string) {
    this.executeApiRequest(
      requestUri,
      /* requestParams= */ {'method': 'delete'},
      /* retryOnFailure= */ true
    );
  }

  /**
   * Returns the API resource name.
   *
   */
  getResourceName(): string {
    return this.resourceName;
  }

  getId(): string {
    throw new Error('Not Allowed.');
  }
}

/**
 * An extension of `DisplayVideoApiClient` to handle {@link Advertiser}
 * resources.
 * @final
 */
export class Advertisers extends DisplayVideoApiClient {
  /**
   * Constructs an instance of `Advertisers`.
   *
   * @param partnerId The DV360 Partner identifier
   */
  constructor(private readonly partnerId: string) {
    super('advertisers');
  }

  /**
   * Retrieves all advertiser resources from the API, filtering them using the
   * given 'filter' and calling the given 'callback' for every retrieved 'page'
   * of data. A typical callback would output every page of retrieved resources
   * directly (to an associated spreadsheet for example). Note: it is not
   * recommended to collect all retrieved resources into a single `Array` as
   * this would highly likely break for accounts with a significantly large
   * number of entities.
   */
  list(
    callback: (p1: Advertiser[]) => void,
    filter: FilterExpression | null = activeEntityFilter(),
    maxPages: number = -1
  ) {
    const filterQueryString = filter
      ? `&filter=${filter.toApiQueryString()}`
      : '';
    super.listResources(
      `advertisers?partnerId=${this.getPartnerId()}${filterQueryString}`,
      callback,
      maxPages
    );
  }

  /**
   * Converts an advertiser resource object returned by the API into a concrete
   * {@link Advertiser} instance.
   *
   * @param resource The API resource object
   * @return The concrete advertiser instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  asDisplayVideoResource(resource: {[key: string]: unknown}): Advertiser {
    return Advertiser.fromApiResource(resource);
  }

  /**
   * Retrieves a single advertiser from the API, identified by 'advertiserId'.
   *
   * @param advertiserId The ID of the advertiser to 'get'
   * @return An object representing the retrieved advertiser
   *     resource
   */
  get(advertiserId: string): Advertiser {
    return super.getResource(`advertisers/${advertiserId}`) as Advertiser;
  }

  /**
   * Creates a new advertiser resource based on the given 'advertiserResource'
   * object.
   */
  create(advertiserResource: DisplayVideoResource): Advertiser {
    return super.createResource(
      'advertisers',
      advertiserResource
    ) as Advertiser;
  }

  /**
   * Modifies an advertiser resource identified by 'advertiserId' based on the
   * given 'changedProperties'.
   *
   * @param advertiserResource The advertiser resource
   *     to 'patch'
   * @param changedProperties A comma-separated list of properties that
   *     have changed in the advertiser resource and therefore need updating
   * @return An object representing the modified advertiser
   *     resource
   */
  patch(
    advertiserResource: DisplayVideoResource,
    changedProperties: string
  ): Advertiser {
    return super.patchResource(
      `advertisers/${advertiserResource.getId()}?updateMask=` +
        encodeURIComponent(changedProperties),
      advertiserResource
    ) as Advertiser;
  }

  /**
   * Deletes an advertiser resource identified by 'advertiserId'.
   *
   * @param advertiserId The ID of the advertiser to 'delete'
   */
  delete(advertiserId: string) {
    super.deleteResource(`advertisers/${advertiserId}`);
  }

  /**
   * Returns the DV360 Partner identifier.
   *
   */
  getPartnerId(): string {
    return this.partnerId;
  }
}

/**
 * An extension of `DisplayVideoApiClient` to handle {@link Campaign} resources.
 * @final
 */
export class Campaigns extends DisplayVideoApiClient {
  /**
   * Constructs an instance of `Campaigns`.
   *
   * @param advertiserId The DV360 Advertiser identifier
   */
  constructor(private readonly advertiserId: string) {
    super('campaigns');
  }

  /**
   * Retrieves all campaign resources from the API, filtering them using the
   * given 'filter' and calling the given 'callback' for every retrieved 'page'
   * of data.
   *
   * @param callback Callback to
   *     trigger after fetching every 'page' of campaigns
   * @param filter Optional filter for filtering retrieved
   *     results. Defaults to filtering for 'active' campaign resources
   * @param maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   */
  list(
    callback: (p1: Campaign[]) => void,
    filter: FilterExpression | null = activeEntityFilter(),
    maxPages: number = -1
  ) {
    const filterQueryString = filter
      ? `?filter=${filter.toApiQueryString()}`
      : '';
    super.listResources(
      `advertisers/${this.getAdvertiserId()}/campaigns${filterQueryString}`,
      callback,
      maxPages
    );
  }

  /**
   * Converts a campaign resource object returned by the API into a concrete
   * {@link Campaign} instance.
   *
   * @param resource The API resource object
   * @return The concrete campaign instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  asDisplayVideoResource(resource: {[key: string]: unknown}): Campaign {
    return Campaign.fromApiResource(resource);
  }

  /**
   * Retrieves a single campaign from the API, identified by 'campaignId'.
   *
   * @param campaignId The ID of the campaign to 'get'
   * @return An object representing the retrieved campaign resource
   */
  get(campaignId: string): Campaign {
    return super.getResource(
      `advertisers/${this.getAdvertiserId()}/campaigns/${campaignId}`
    ) as Campaign;
  }

  /**
   * Creates a new campaign resource based on the given 'campaignResource'
   * object.
   *
   * @param campaignResource The campaign resource to
   *     create
   * @return An object representing the created campaign resource
   */
  create(campaignResource: DisplayVideoResource): Campaign {
    return super.createResource(
      `advertisers/${this.getAdvertiserId()}/campaigns`,
      campaignResource
    ) as Campaign;
  }

  /**
   * Modifies a campaign resource identified by 'campaignId' based on the
   * given 'changedProperties'.
   *
   * @param campaignResource The campaign resource to
   *     'patch'
   * @param changedProperties A comma-separated list of properties that
   *     have changed in the campaign resource and therefore need updating
   * @return An object representing the modified campaign resource
   */
  patch(
    campaignResource: DisplayVideoResource,
    changedProperties: string
  ): Campaign {
    return super.patchResource(
      `advertisers/${this.getAdvertiserId()}/campaigns/` +
        `${campaignResource.getId()}?updateMask=` +
        encodeURIComponent(changedProperties),
      campaignResource
    ) as Campaign;
  }

  /**
   * Deletes a campaign resource identified by 'campaignId'.
   *
   * @param campaignId The ID of the campaign to 'delete'
   */
  delete(campaignId: string) {
    super.deleteResource(
      `advertisers/${this.getAdvertiserId()}/campaigns/${campaignId}`
    );
  }

  /**
   * Returns the DV360 Advertiser identifier.
   *
   */
  getAdvertiserId(): string {
    return this.advertiserId;
  }
}

/**
 * An extension of `DisplayVideoApiClient` to handle {@link InsertionOrder}
 * resources.
 * @final
 */
export class InsertionOrders extends DisplayVideoApiClient {
  /**
   * Constructs an instance of `InsertionOrders`.
   *
   * @param advertiserId The DV360 Advertiser identifier
   */
  constructor(private readonly advertiserId: string) {
    super('insertionOrders');
  }

  /**
   * Retrieves all insertion order resources from the API, filtering them using
   * the given 'filter' and calling the given 'callback' for every retrieved
   * 'page' of data.
   *
   * @param callback Callback to
   *     trigger after fetching every 'page' of insertion orders
   * @param filter Optional filter for filtering retrieved
   *     results. Defaults to filtering for 'active' insertion order resources
   * @param maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   */
  list(
    callback: (p1: InsertionOrder[]) => void,
    filter: FilterExpression | null = activeEntityFilter(),
    maxPages: number = -1
  ) {
    const filterQueryString = filter
      ? `?filter=${filter.toApiQueryString()}`
      : '';
    super.listResources(
      `advertisers/${this.getAdvertiserId()}/` +
        `insertionOrders${filterQueryString}`,
      callback,
      maxPages
    );
  }

  /**
   * Converts an insertion order resource object returned by the API into a
   * concrete {@link InsertionOrder} instance.
   *
   * @param resource The API resource object
   * @return The concrete insertion order instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  asDisplayVideoResource(resource: {[key: string]: unknown}): InsertionOrder {
    return InsertionOrder.fromApiResource(resource);
  }

  /**
   * Retrieves a single insertion order from the API, identified by
   * 'insertionOrderId'.
   *
   * @param insertionOrderId The ID of the insertion order to 'get'
   * @return An object representing the retrieved
   *     insertion order resource
   */
  get(insertionOrderId: string): InsertionOrder {
    return super.getResource(
      `advertisers/${this.getAdvertiserId()}/` +
        `insertionOrders/${insertionOrderId}`
    ) as InsertionOrder;
  }

  /**
   * Creates a new insertion order resource based on the given
   * 'insertionOrderResource' object.
   *
   * @param insertionOrderResource The insertion order
   *     resource to create
   * @return An object representing the created insertion
   *     order resource
   */
  create(insertionOrderResource: DisplayVideoResource): InsertionOrder {
    return super.createResource(
      `advertisers/${this.getAdvertiserId()}/insertionOrders`,
      insertionOrderResource
    ) as InsertionOrder;
  }

  /**
   * Modifies an insertion order resource identified by 'insertionOrderId' based
   * on the given 'changedProperties'.
   *
   * @param insertionOrderResource The insertion order
   *     resource to 'patch'
   * @param changedProperties A comma-separated list of properties that
   *     have changed in the insertion order resource and therefore need
   *     updating
   * @return An object representing the modified
   *     insertion order resource
   */
  patch(
    insertionOrderResource: DisplayVideoResource,
    changedProperties: string
  ): InsertionOrder {
    return super.patchResource(
      `advertisers/${this.getAdvertiserId()}/insertionOrders/` +
        `${insertionOrderResource.getId()}?updateMask=` +
        encodeURIComponent(changedProperties),
      insertionOrderResource
    ) as InsertionOrder;
  }

  /**
   * Deletes an insertion order resource identified by 'insertionOrderId'.
   *
   * @param insertionOrderId The ID of the insertion order to 'delete'
   */
  delete(insertionOrderId: string) {
    super.deleteResource(
      `advertisers/${this.getAdvertiserId()}/` +
        `insertionOrders/${insertionOrderId}`
    );
  }

  /**
   * Returns the DV360 Advertiser identifier.
   *
   */
  getAdvertiserId(): string {
    return this.advertiserId;
  }
}

/**
 * An extension of `DisplayVideoApiClient` to handle {@link LineItem} resources.
 * @final
 */
export class LineItems extends DisplayVideoApiClient {
  /**
   * Constructs an instance of `LineItems`.
   *
   * @param advertiserId The DV360 Advertiser identifier
   */
  constructor(private readonly advertiserId: string) {
    super('lineItems');
  }

  /**
   * Retrieves all line item resources from the API, filtering them using
   * the given 'filter' and calling the given 'callback' for every retrieved
   * 'page' of data.
   *
   * @param callback Callback to
   *     trigger after fetching every 'page' of line items
   * @param filter Optional filter for filtering retrieved
   *     results. Defaults to filtering for 'active' line item resources
   * @param maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   */
  list(
    callback: (p1: InsertionOrder[]) => void,
    filter: FilterExpression | null = activeEntityFilter(),
    maxPages: number = -1
  ) {
    const filterQueryString = filter
      ? `?filter=${filter.toApiQueryString()}`
      : '';
    super.listResources(
      `advertisers/${this.getAdvertiserId()}/lineItems${filterQueryString}`,
      callback,
      maxPages
    );
  }

  /**
   * Converts a line item resource object returned by the API into a concrete
   * {@link LineItem} instance.
   *
   * @param resource The API resource object
   * @return The concrete line item instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  asDisplayVideoResource(resource: {[key: string]: unknown}): LineItem {
    return LineItem.fromApiResource(resource);
  }

  /**
   * Retrieves a single line item from the API, identified by 'lineItemId'.
   *
   * @param lineItemId The ID of the line item to 'get'
   * @return An object representing the retrieved line item resource
   */
  get(lineItemId: string): LineItem {
    return super.getResource(
      `advertisers/${this.getAdvertiserId()}/lineItems/${lineItemId}`
    ) as LineItem;
  }

  /**
   * Creates a new line item resource based on the given 'lineItemResource'
   * object.
   *
   * @param lineItemResource The line item resource to
   *     create
   * @return An object representing the created line item resource
   */
  create(lineItemResource: DisplayVideoResource): LineItem {
    return super.createResource(
      `advertisers/${this.getAdvertiserId()}/lineItems`,
      lineItemResource
    ) as LineItem;
  }

  /**
   * Modifies a line item resource identified by 'lineItemId' based on the given
   * 'changedProperties'.
   *
   * @param lineItemResource The line item resource to
   *     'patch'
   * @param changedProperties A comma-separated list of properties that
   *     have changed in the line item resource and therefore need updating
   * @return An object representing the modified line item resource
   */
  patch(
    lineItemResource: DisplayVideoResource,
    changedProperties: string
  ): LineItem {
    return super.patchResource(
      `advertisers/${this.getAdvertiserId()}/lineItems/` +
        `${lineItemResource.getId()}?updateMask=` +
        encodeURIComponent(changedProperties),
      lineItemResource
    ) as LineItem;
  }

  /**
   * Deletes a line item resource identified by 'lineItemId'.
   *
   * @param lineItemId The ID of the line item to 'delete'
   */
  delete(lineItemId: string) {
    super.deleteResource(
      `advertisers/${this.getAdvertiserId()}/lineItems/${lineItemId}`
    );
  }

  /**
   * Returns the DV360 Advertiser identifier.
   *
   */
  getAdvertiserId(): string {
    return this.advertiserId;
  }
}

/**
 * An extension of `DisplayVideoApiClient` to handle {@link InventorySource}
 * resources.
 * @final
 */
export class InventorySources extends DisplayVideoApiClient {
  /**
   * Constructs an instance of `InventorySources`.
   *
   * @param partnerId The DV360 Partner identifier to fetch inventory
   *     sources for
   * @param advertiserId Optional DV360 Advertiser identifier. If
   *     provided will be used for fetching inventory sources instead of the
   *     'partnerId'
   */
  constructor(private readonly partnerId: string, private readonly advertiserId: string | null = null) {
    super('inventorySources');
  }

  /**
   * Retrieves all inventory source resources from the API, filtering them using
   * the given 'filter' and calling the given 'callback' for every retrieved
   * 'page' of data.
   *
   * @param callback Callback to
   *     trigger after fetching every 'page' of inventory sources
   * @param filter Optional filter for filtering retrieved
   *     results. Defaults to filtering for 'active' inventory source resources
   * @param maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   */
  list(
    callback: (p1: InventorySource[]) => void,
    filter: FilterExpression | null = activeEntityFilter(),
    maxPages: number = -1
  ) {
    const [key, value] = this.getAdvertiserId()
      ? ['advertiserId', this.getAdvertiserId()]
      : ['partnerId', this.getPartnerId()];
    const filterQueryString = filter
      ? `&filter=${filter.toApiQueryString()}`
      : '';
    super.listResources(
      `inventorySources?${key}=${value}${filterQueryString}`,
      callback,
      maxPages
    );
  }

  /**
   * Converts an inventory source resource object returned by the API into a
   * concrete {@link InventorySource} instance.
   *
   * @param resource The API resource object
   * @return The concrete inventory source instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  asDisplayVideoResource(resource: {[key: string]: unknown}): InventorySource {
    return InventorySource.fromApiResource(resource);
  }

  /**
   * Retrieves a single inventory source from the API, identified by
   * 'inventorySourceId'.
   *
   * @param inventorySourceId The ID of the inventory source to 'get'
   * @return An object representing the retrieved inventory
   *     source resource
   */
  get(inventorySourceId: string): InventorySource {
    return super.getResource(
      `inventorySources/${inventorySourceId}` +
        `?partnerId=${this.getPartnerId()}`
    ) as InventorySource;
  }

  /**
   * @throws {!Error} As this method is not allowed for this type
   */
  override createResource(
    requestUri: string,
    payload: DisplayVideoResource
  ): DisplayVideoResource {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * @throws {!Error} As this method is not allowed for this type
   */
  override patchResource(requestUri: string): DisplayVideoResource {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * @throws {!Error} As this method is not allowed for this type
   */
  override patchResourceByComparison(
    requestUri: string,
    original: DisplayVideoResource,
    modified: DisplayVideoResource | null
  ): DisplayVideoResource {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * @throws {!Error} As this method is not allowed for this type
   */
  override deleteResource(requestUri: string) {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * Returns the DV360 Partner identifier.
   *
   */
  getPartnerId(): string {
    return this.partnerId;
  }

  /**
   * Returns the optional DV360 Advertiser identifier.
   *
   */
  getAdvertiserId(): string | null {
    return this.advertiserId;
  }
}

/**
 * An extension of `DisplayVideoApiClient` to handle {@link TargetingOption}
 * resources.
 * @final
 */
export class TargetingOptions extends DisplayVideoApiClient {
  /**
   * Constructs an instance of `TargetingOptions`.
   *
   * @param targetingType The targeting type for retrieving
   *     targeting options
   * @param advertiserId The DV360 advertiser identifier to use for
   *     retrieving targeting options
   */
  constructor(private readonly targetingType: TargetingType, private readonly advertiserId: string) {
    super('targetingOptions');
  }

  /**
   * Retrieves all targeting option resources from the API, filtering them using
   * the given 'filter' and calling the given 'callback' for every retrieved
   * 'page' of data.
   *
   * @param callback Callback to
   *     trigger after fetching every 'page' of targeting options
   * @param filter Optional filter for filtering retrieved
   *     results. Defaults to null
   * @param maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   */
  list(
    callback: (p1: TargetingOption[]) => void,
    filter: FilterExpression | null = null,
    maxPages: number = -1
  ) {
    const filterQueryString = filter
      ? `&filter=${filter.toApiQueryString()}`
      : '';
    super.listResources(
      `targetingTypes/${this.getTargetingType()}/targetingOptions` +
        `?advertiserId=${this.getAdvertiserId()}${filterQueryString}`,
      callback,
      maxPages
    );
  }

  /**
   * Searches the API for targeting options of a given type based on the given
   * search query and calls the given 'callback' for every retrieved 'page' of
   * data. Only {@link TARGETING_TYPE.GEO_REGION} is supported. The API
   * explicitly provides this method as 'filter' values for the 'list' method
   * for targeting options only support the 'EQ' rule operator, and search
   * queries do not have to be an exact match (e.g. a search query of "New "
   * would yield both "New Jersey" and "New York").
   *
   * @param query The search query
   * @param callback Callback to
   *     trigger after fetching every 'page' of targeting options
   * @param maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   * @throws {!Error} If the targeting type is not GEO_REGION
   */
  search(
    query: string,
    callback: (p1: TargetingOption[]) => void,
    maxPages: number = -1
  ) {
    if (this.getTargetingType() !== TARGETING_TYPE.GEO_REGION) {
      throw new Error(
        `Error! "search" is only supported for ${TARGETING_TYPE.GEO_REGION}`
      );
    }
    const url =
      `targetingTypes/${this.getTargetingType()}/` +
      encodeURIComponent('targetingOptions:search');
    const payload = {
      advertiserId: this.getAdvertiserId(),
      geoRegionSearchTerms: {geoRegionQuery: query},
    };
    const params = {'method': 'post', 'payload': JSON.stringify(payload)};
    super.listResources(url, callback, maxPages, params);
  }

  /**
   * Converts a targeting option resource object returned by the API into a
   * concrete {@link TargetingOption} instance.
   *
   * @param resource The API resource object
   * @return The concrete targeting option instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  asDisplayVideoResource(resource: {[key: string]: unknown}): TargetingOption {
    return TargetingOption.fromApiResource(resource);
  }

  /**
   * Retrieves a single targeting option from the API, identified by
   * 'targetingOptionId'.
   *
   * @param targetingOptionId The ID of the targeting option to 'get'
   * @return An object representing the retrieved targeting
   *     option resource
   */
  get(targetingOptionId: string): TargetingOption {
    return super.getResource(
      `targetingTypes/${this.getTargetingType()}/targetingOptions/` +
        `${targetingOptionId}?advertiserId=${this.getAdvertiserId()}`
    ) as TargetingOption;
  }

  /**
   * @throws {!Error} As this method is not allowed for this type
   */
  override createResource(
    requestUri: string,
    payload: DisplayVideoResource
  ): DisplayVideoResource {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * @throws {!Error} As this method is not allowed for this type
   */
  override patchResource(requestUri: string): DisplayVideoResource {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * @throws {!Error} As this method is not allowed for this type
   */
  override patchResourceByComparison(
    requestUri: string,
    original: DisplayVideoResource,
    modified: DisplayVideoResource | null
  ): DisplayVideoResource {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * @throws {!Error} As this method is not allowed for this type
   */
  override deleteResource(requestUri: string) {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * Returns the targeting type.
   *
   */
  getTargetingType(): TargetingType {
    return this.targetingType;
  }

  /**
   * Returns the DV360 Advertiser identifier.
   *
   */
  getAdvertiserId(): string {
    return this.advertiserId;
  }
}

interface AssignedTargetingOptionsParams {
  campaignId?: string | null;
  insertionOrderId?: string | null;
  lineItemId?: string | null;
}

/**
 * An extension of `DisplayVideoApiClient` to handle
 * {@link AssignedTargetingOption} resources.
 * @final
 */
export class AssignedTargetingOptions extends DisplayVideoApiClient {

  private readonly campaignId: string | null;

  private readonly insertionOrderId: string | null;

  private readonly lineItemId: string | null;

  /**
   * Assigned targeting options are read-only (list & get operations only) for
   * campaigns and insertion orders.
   *
   */
  private readonly readOnly: boolean;

  /**
   * Constructs an instance of `AssignedTargetingOptions`.
   */
  constructor(
    private readonly targetingType: TargetingType,
    private readonly advertiserId: string,
    {
      campaignId = null,
      insertionOrderId = null,
      lineItemId = null,
    }: AssignedTargetingOptionsParams = {}
  ) {
    super('assignedTargetingOptions');

    this.campaignId = campaignId;

    this.insertionOrderId = insertionOrderId;

    this.lineItemId = lineItemId;

    /**
     * Assigned targeting options are read-only (list & get operations only) for
     * campaigns and insertion orders.
     *
     */
    this.readOnly = campaignId != null || insertionOrderId != null;
  }

  /**
   * Returns the base url string for every API operation. Checks the initialized
   * constructor parameters and adds the necessary extensions to the resulting
   * string.
   *
   * @return The base url for every API operation
   */
  getBaseUrl(): string {
    const prefix = `advertisers/${this.getAdvertiserId()}/`;
    const suffix =
      `targetingTypes/${this.getTargetingType()}/` + `assignedTargetingOptions`;
    let extension = '';

    if (this.getCampaignId()) {
      extension = `campaigns/${this.getCampaignId()}/`;
    } else if (this.getInsertionOrderId()) {
      extension = `insertionOrders/${this.getInsertionOrderId()}/`;
    } else if (this.getLineItemId()) {
      extension = `lineItems/${this.getLineItemId()}/`;
    }
    return prefix + extension + suffix;
  }

  /**
   * Retrieves all assigned targeting option resources from the API, filtering
   * them using the given 'filter' and calling the given 'callback' for every
   * retrieved 'page' of data.
   *
   * @param callback Callback to trigger after fetching every 'page' of assigned targeting
   *     options
   * @param filter Optional filter for filtering retrieved
   *     results. Defaults to null
   * @param maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   */
  list(
    callback: (p1: AssignedTargetingOption[]) => void,
    filter: FilterExpression | null = null,
    maxPages: number = -1
  ) {
    const filterQueryString = filter
      ? `?filter=${filter.toApiQueryString()}`
      : '';
    super.listResources(
      this.getBaseUrl() + filterQueryString,
      callback,
      maxPages
    );
  }

  /**
   * Converts an assigned targeting option resource object returned by the API
   * into a concrete {@link AssignedTargetingOption} instance.
   *
   * @param resource The API resource object
   * @return The concrete assigned targeting option
   *     instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   */
  asDisplayVideoResource(resource: {
    [key: string]: unknown;
  }): AssignedTargetingOption {
    return AssignedTargetingOption.fromApiResource(resource);
  }

  /**
   * Retrieves a single assigned targeting option from the API, identified by
   * 'assignedTargetingOptionId'.
   *
   * @param assignedTargetingOptionId The ID of the assigned targeting
   *     option to 'get'
   * @return An object representing the retrieved
   *     assigned targeting option resource
   */
  get(assignedTargetingOptionId: string): AssignedTargetingOption {
    return super.getResource(
      `${this.getBaseUrl()}/${assignedTargetingOptionId}`
    ) as AssignedTargetingOption;
  }

  /**
   * Creates a new assigned targeting option resource based on the given
   * 'assignedTargetingOptionResource' object.
   *
   * @param assignedTargetingOptionResource The
   *     assigned targeting option resource to create
   * @return An object representing the created
   *     assigned targeting option resource
   * @throws {!Error} If this method is not allowed for this type
   */
  create(
    assignedTargetingOptionResource: DisplayVideoResource
  ): AssignedTargetingOption {
    return this.createResource(
      this.getBaseUrl(),
      assignedTargetingOptionResource
    ) as AssignedTargetingOption;
  }

  /**
   * @throws {!Error} If this method is not allowed for this type
   */
  override createResource(
    requestUri: string,
    payload: DisplayVideoResource
  ): DisplayVideoResource {
    if (this.isReadOnly()) {
      throw new Error('405 Method Not Allowed');
    }
    return super.createResource(requestUri, payload);
  }

  /**
   * @throws {!Error} As this method is not allowed for this type
   */
  override patchResource(requestUri: string): DisplayVideoResource {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * @throws {!Error} As this method is not allowed for this type
   */
  override patchResourceByComparison(
    requestUri: string,
    original: DisplayVideoResource,
    modified: DisplayVideoResource | null
  ): DisplayVideoResource {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * Deletes an assigned targeting option identified by
   * 'assignedTargetingOptionId'.
   */
  delete(assignedTargetingOptionId: string) {
    this.deleteResource(`${this.getBaseUrl()}/${assignedTargetingOptionId}`);
  }

  /**
   * @throws {!Error} If this method is not allowed for this type
   */
  override deleteResource(requestUri: string) {
    if (this.isReadOnly()) {
      throw new Error('405 Method Not Allowed');
    }
    super.deleteResource(requestUri);
  }

  /**
   * Returns the targeting type.
   *
   */
  getTargetingType(): TargetingType {
    return this.targetingType;
  }

  /**
   * Returns the DV360 advertiser identifier.
   *
   */
  getAdvertiserId(): string {
    return this.advertiserId;
  }

  /**
   * Returns the DV360 campaign identifier.
   *
   */
  getCampaignId(): string | null {
    return this.campaignId;
  }

  /**
   * Returns the DV360 insertion order identifier.
   *
   */
  getInsertionOrderId(): string | null {
    return this.insertionOrderId;
  }

  /**
   * Returns the DV360 line item identifier.
   *
   */
  getLineItemId(): string | null {
    return this.lineItemId;
  }

  /**
   * Whether this API client is read only (only list and get operations are
   * supported) or not.
   *
   */
  isReadOnly(): boolean {
    return this.readOnly;
  }
}
