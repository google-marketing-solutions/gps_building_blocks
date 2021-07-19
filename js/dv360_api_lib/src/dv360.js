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

/** @const {string} */
const API_SCOPE = 'displayvideo';

/** @const {string} */
const API_VERSION = 'v1';

/**
 * Returns a `FilterExpression` for active entities.
 *
 * @return {!FilterExpression}
 */
function activeEntityFilter() {
  return new FilterExpression(
      [new Rule('entityStatus', RuleOperator.EQ, Status.ACTIVE)]);
}

/**
 * An abstract API client for the DV360 API that extends `BaseApiClient`.
 * Provides base CRUD operations that are used by other resource-specific
 * extension classes to manipulate specific API resources.
 * @abstract
 */
class DisplayVideoApiClient extends BaseApiClient {
  /** Constructs an instance of `DisplayVideoApiClient`. */
  constructor(resourceName) {
    super(API_SCOPE, API_VERSION);

    /** @private @const {string} */
    this.resourceName_ = resourceName;
  }

  /**
   * Retrieves all resources for the given 'requestUri' from the API, calling
   * 'requestCallback' for every retrieved 'page' of data (i.e. this function
   * has no return value).
   *
   * @param {string} requestUri The URI of the request
   * @param {function(!Array<!DisplayVideoResource>): undefined}
   *     requestCallback The callback to trigger after fetching every 'page' of
   *     results
   * @param {number=} maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   * @param {!Object<string, string>=} requestParams Optional requestParams to
   *     use for the request
   */
  listResources(requestUri, requestCallback, maxPages = -1, requestParams = {
    method: 'get'
  }) {
    this.executePagedApiRequest(
        requestUri, requestParams,
        /* requestCallback= */ (apiResponse) => {
          const apiResources = apiResponse[this.getResourceName()];

          if (Array.isArray(apiResources)) {
            const resources = /** !Array<!Object<string, *>> */ apiResources
                .map((resource) => this.asDisplayVideoResource(resource));
            requestCallback(resources);
          }
        }, maxPages);
  }

  /**
   * Converts a resource object returned by the API into a concrete
   * {@link DisplayVideoResource} instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!DisplayVideoResource} The concrete instance
   * @abstract
   */
  asDisplayVideoResource(resource) {}

  /**
   * Retrieves a single resource from the API. All required information
   * will already be provided within the given 'requestUri'.
   *
   * @param {string} requestUri The URI of the GET request
   * @return {!DisplayVideoResource} An object representing the retrieved API
   *     resource
   */
  getResource(requestUri) {
    return this.asDisplayVideoResource(this.executeApiRequest(
        requestUri,
        /* requestParams= */ {method: 'get'},
        /* retryOnFailure= */ true));
  }

  /**
   * Creates an instance of the given API resource, described by 'payload'.
   *
   * @param {string} requestUri The URI of the POST request
   * @param {!DisplayVideoResource} payload The representation of the resource
   *     to create
   * @return {!DisplayVideoResource} The created resource
   */
  createResource(requestUri, payload) {
    return this.asDisplayVideoResource(this.executeApiRequest(
        requestUri,
        /* requestParams= */ {
          method: 'post',
          payload: JSON.stringify(payload),
        },
        /* retryOnFailure= */ false));
  }

  /**
   * Modifies an API resource. All required information will already be provided
   * within the given 'requestUri'.
   *
   * @param {string} requestUri The URI of the PATCH request
   * @param {!DisplayVideoResource} payload The representation of the resource
   *     to patch
   * @return {!DisplayVideoResource} The updated resource
   */
  patchResource(requestUri, payload) {
    return this.asDisplayVideoResource(this.executeApiRequest(
        requestUri,
        /* requestParams= */ {
          method: 'patch',
          payload: JSON.stringify(payload),
        },
        /* retryOnFailure= */ true));
  }

  /**
   * Modifies the 'original' resource by identifing properties that have changed
   * after comparing 'original' to its 'modified' counterpart.
   *
   * @param {string} requestUri The URI of the PATCH request
   * @param {!DisplayVideoResource} original The original resource
   * @param {?DisplayVideoResource} modified The modified resource
   * @return {!DisplayVideoResource} An object representing the modified
   *     resource
   */
  patchResourceByComparison(requestUri, original, modified) {
    const changedProperties = original.getChangedPropertiesString(modified);

    return this.patchResource(
        UriUtil.modifyUrlQueryString(
            requestUri, 'updateMask', encodeURIComponent(changedProperties)),
        original);
  }

  /**
   * Deletes an instance of the given API resource. All required information
   * will already be provided within the given 'requestUri'.
   *
   * @param {string} requestUri The URI of the DELETE request
   */
  deleteResource(requestUri) {
    this.executeApiRequest(
        requestUri,
        /* requestParams= */ {method: 'delete'},
        /* retryOnFailure= */ true);
  }

  /**
   * Returns the API resource name.
   *
   * @return {string}
   */
  getResourceName() {
    return this.resourceName_;
  }
}

/**
 * An extension of `DisplayVideoApiClient` to handle {@link Advertiser}
 * resources.
 * @final
 */
class Advertisers extends DisplayVideoApiClient {
  /**
   * Constructs an instance of `Advertisers`.
   *
   * @param {string} partnerId The DV360 Partner identifier
   */
  constructor(partnerId) {
    super('advertisers');

    /** @private @const {string} */
    this.partnerId_ = partnerId;
  }

  /**
   * Retrieves all advertiser resources from the API, filtering them using the
   * given 'filter' and calling the given 'callback' for every retrieved 'page'
   * of data. A typical callback would output every page of retrieved resources
   * directly (to an associated spreadsheet for example). Note: it is not
   * recommended to collect all retrieved resources into a single `Array` as
   * this would highly likely break for accounts with a significantly large
   * number of entities.
   *
   * @param {function(!Array<!Advertiser>): undefined} callback Callback to
   *     trigger after fetching every 'page' of advertisers
   * @param {?FilterExpression=} filter Optional filter for filtering retrieved
   *     results. Defaults to filtering for 'active' advertiser resources
   * @param {number=} maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   */
  list(callback, filter = activeEntityFilter(), maxPages = -1) {
    const filterQueryString =
        filter ? `&filter=${filter.toApiQueryString()}` : '';
    super.listResources(
        `advertisers?partnerId=${this.getPartnerId()}${filterQueryString}`,
        callback, maxPages);
  }

  /**
   * Converts an advertiser resource object returned by the API into a concrete
   * {@link Advertiser} instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!Advertiser} The concrete advertiser instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   * @override
   */
  asDisplayVideoResource(resource) {
    return Advertiser.fromApiResource(resource);
  }

  /**
   * Retrieves a single advertiser from the API, identified by 'advertiserId'.
   *
   * @param {string} advertiserId The ID of the advertiser to 'get'
   * @return {!Advertiser} An object representing the retrieved advertiser
   *     resource
   */
  get(advertiserId) {
    return /** @type {!Advertiser} */ (
        super.getResource(`advertisers/${advertiserId}`));
  }

  /**
   * Creates a new advertiser resource based on the given 'advertiserResource'
   * object.
   *
   * @param {!DisplayVideoResource} advertiserResource The advertiser resource
   *     to create
   * @return {!Advertiser} An object representing the created advertiser
   *     resource
   */
  create(advertiserResource) {
    return /** @type {!Advertiser} */ (
        super.createResource('advertisers', advertiserResource));
  }

  /**
   * Modifies an advertiser resource identified by 'advertiserId' based on the
   * given 'changedProperties'.
   *
   * @param {!DisplayVideoResource} advertiserResource The advertiser resource
   *     to 'patch'
   * @param {string} changedProperties A comma-separated list of properties that
   *     have changed in the advertiser resource and therefore need updating
   * @return {!Advertiser} An object representing the modified advertiser
   *     resource
   */
  patch(advertiserResource, changedProperties) {
    return /** @type {!Advertiser} */ (super.patchResource(
        `advertisers/${advertiserResource.getId()}?updateMask=` +
            encodeURIComponent(changedProperties),
        advertiserResource));
  }

  /**
   * Deletes an advertiser resource identified by 'advertiserId'.
   *
   * @param {string} advertiserId The ID of the advertiser to 'delete'
   */
  delete(advertiserId) {
    super.deleteResource(`advertisers/${advertiserId}`);
  }

  /**
   * Returns the DV360 Partner identifier.
   *
   * @return {string}
   */
  getPartnerId() {
    return this.partnerId_;
  }
}

/**
 * An extension of `DisplayVideoApiClient` to handle {@link Campaign} resources.
 * @final
 */
class Campaigns extends DisplayVideoApiClient {
  /**
   * Constructs an instance of `Campaigns`.
   *
   * @param {string} advertiserId The DV360 Advertiser identifier
   */
  constructor(advertiserId) {
    super('campaigns');

    /** @private @const {string} */
    this.advertiserId_ = advertiserId;
  }

  /**
   * Retrieves all campaign resources from the API, filtering them using the
   * given 'filter' and calling the given 'callback' for every retrieved 'page'
   * of data.
   *
   * @param {function(!Array<!Campaign>): undefined} callback Callback to
   *     trigger after fetching every 'page' of campaigns
   * @param {?FilterExpression=} filter Optional filter for filtering retrieved
   *     results. Defaults to filtering for 'active' campaign resources
   * @param {number=} maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   */
  list(callback, filter = activeEntityFilter(), maxPages = -1) {
    const filterQueryString =
        filter ? `?filter=${filter.toApiQueryString()}` : '';
    super.listResources(
        `advertisers/${this.getAdvertiserId()}/campaigns${filterQueryString}`,
        callback, maxPages);
  }

  /**
   * Converts a campaign resource object returned by the API into a concrete
   * {@link Campaign} instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!Campaign} The concrete campaign instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   * @override
   */
  asDisplayVideoResource(resource) {
    return Campaign.fromApiResource(resource);
  }

  /**
   * Retrieves a single campaign from the API, identified by 'campaignId'.
   *
   * @param {string} campaignId The ID of the campaign to 'get'
   * @return {!Campaign} An object representing the retrieved campaign resource
   */
  get(campaignId) {
    return /** @type {!Campaign} */ (super.getResource(
        `advertisers/${this.getAdvertiserId()}/campaigns/${campaignId}`));
  }

  /**
   * Creates a new campaign resource based on the given 'campaignResource'
   * object.
   *
   * @param {!DisplayVideoResource} campaignResource The campaign resource to
   *     create
   * @return {!Campaign} An object representing the created campaign resource
   */
  create(campaignResource) {
    return /** @type {!Campaign} */ (super.createResource(
        `advertisers/${this.getAdvertiserId()}/campaigns`, campaignResource));
  }

  /**
   * Modifies a campaign resource identified by 'campaignId' based on the
   * given 'changedProperties'.
   *
   * @param {!DisplayVideoResource} campaignResource The campaign resource to
   *     'patch'
   * @param {string} changedProperties A comma-separated list of properties that
   *     have changed in the campaign resource and therefore need updating
   * @return {!Campaign} An object representing the modified campaign resource
   */
  patch(campaignResource, changedProperties) {
    return /** @type {!Campaign} */ (super.patchResource(
        `advertisers/${this.getAdvertiserId()}/campaigns/` +
            `${campaignResource.getId()}?updateMask=` +
            encodeURIComponent(changedProperties),
        campaignResource));
  }

  /**
   * Deletes a campaign resource identified by 'campaignId'.
   *
   * @param {string} campaignId The ID of the campaign to 'delete'
   */
  delete(campaignId) {
    super.deleteResource(
        `advertisers/${this.getAdvertiserId()}/campaigns/${campaignId}`);
  }

  /**
   * Returns the DV360 Advertiser identifier.
   *
   * @return {string}
   */
  getAdvertiserId() {
    return this.advertiserId_;
  }
}

/**
 * An extension of `DisplayVideoApiClient` to handle {@link InsertionOrder}
 * resources.
 * @final
 */
class InsertionOrders extends DisplayVideoApiClient {
  /**
   * Constructs an instance of `InsertionOrders`.
   *
   * @param {string} advertiserId The DV360 Advertiser identifier
   */
  constructor(advertiserId) {
    super('insertionOrders');

    /** @private @const {string} */
    this.advertiserId_ = advertiserId;
  }

  /**
   * Retrieves all insertion order resources from the API, filtering them using
   * the given 'filter' and calling the given 'callback' for every retrieved
   * 'page' of data.
   *
   * @param {function(!Array<!InsertionOrder>): undefined} callback Callback to
   *     trigger after fetching every 'page' of insertion orders
   * @param {?FilterExpression=} filter Optional filter for filtering retrieved
   *     results. Defaults to filtering for 'active' insertion order resources
   * @param {number=} maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   */
  list(callback, filter = activeEntityFilter(), maxPages = -1) {
    const filterQueryString =
        filter ? `?filter=${filter.toApiQueryString()}` : '';
    super.listResources(
        `advertisers/${this.getAdvertiserId()}/` +
            `insertionOrders${filterQueryString}`,
        callback, maxPages);
  }

  /**
   * Converts an insertion order resource object returned by the API into a
   * concrete {@link InsertionOrder} instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!InsertionOrder} The concrete insertion order instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   * @override
   */
  asDisplayVideoResource(resource) {
    return InsertionOrder.fromApiResource(resource);
  }

  /**
   * Retrieves a single insertion order from the API, identified by
   * 'insertionOrderId'.
   *
   * @param {string} insertionOrderId The ID of the insertion order to 'get'
   * @return {!InsertionOrder} An object representing the retrieved
   *     insertion order resource
   */
  get(insertionOrderId) {
    return /** @type {!InsertionOrder} */ (super.getResource(
        `advertisers/${this.getAdvertiserId()}/` +
        `insertionOrders/${insertionOrderId}`));
  }

  /**
   * Creates a new insertion order resource based on the given
   * 'insertionOrderResource' object.
   *
   * @param {!DisplayVideoResource} insertionOrderResource The insertion order
   *     resource to create
   * @return {!InsertionOrder} An object representing the created insertion
   *     order resource
   */
  create(insertionOrderResource) {
    return /** @type {!InsertionOrder} */ (super.createResource(
        `advertisers/${this.getAdvertiserId()}/insertionOrders`,
        insertionOrderResource));
  }

  /**
   * Modifies an insertion order resource identified by 'insertionOrderId' based
   * on the given 'changedProperties'.
   *
   * @param {!DisplayVideoResource} insertionOrderResource The insertion order
   *     resource to 'patch'
   * @param {string} changedProperties A comma-separated list of properties that
   *     have changed in the insertion order resource and therefore need
   *     updating
   * @return {!InsertionOrder} An object representing the modified
   *     insertion order resource
   */
  patch(insertionOrderResource, changedProperties) {
    return /** @type {!InsertionOrder} */ (super.patchResource(
        `advertisers/${this.getAdvertiserId()}/insertionOrders/` +
            `${insertionOrderResource.getId()}?updateMask=` +
            encodeURIComponent(changedProperties),
        insertionOrderResource));
  }

  /**
   * Deletes an insertion order resource identified by 'insertionOrderId'.
   *
   * @param {string} insertionOrderId The ID of the insertion order to 'delete'
   */
  delete(insertionOrderId) {
    super.deleteResource(
        `advertisers/${this.getAdvertiserId()}/` +
        `insertionOrders/${insertionOrderId}`);
  }

  /**
   * Returns the DV360 Advertiser identifier.
   *
   * @return {string}
   */
  getAdvertiserId() {
    return this.advertiserId_;
  }
}

/**
 * An extension of `DisplayVideoApiClient` to handle {@link LineItem} resources.
 * @final
 */
class LineItems extends DisplayVideoApiClient {
  /**
   * Constructs an instance of `LineItems`.
   *
   * @param {string} advertiserId The DV360 Advertiser identifier
   */
  constructor(advertiserId) {
    super('lineItems');

    /** @private @const {string} */
    this.advertiserId_ = advertiserId;
  }

  /**
   * Retrieves all line item resources from the API, filtering them using
   * the given 'filter' and calling the given 'callback' for every retrieved
   * 'page' of data.
   *
   * @param {function(!Array<!InsertionOrder>): undefined} callback Callback to
   *     trigger after fetching every 'page' of line items
   * @param {?FilterExpression=} filter Optional filter for filtering retrieved
   *     results. Defaults to filtering for 'active' line item resources
   * @param {number=} maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   */
  list(callback, filter = activeEntityFilter(), maxPages = -1) {
    const filterQueryString =
        filter ? `?filter=${filter.toApiQueryString()}` : '';
    super.listResources(
        `advertisers/${this.getAdvertiserId()}/lineItems${filterQueryString}`,
        callback, maxPages);
  }

  /**
   * Converts a line item resource object returned by the API into a concrete
   * {@link LineItem} instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!LineItem} The concrete line item instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   * @override
   */
  asDisplayVideoResource(resource) {
    return LineItem.fromApiResource(resource);
  }

  /**
   * Retrieves a single line item from the API, identified by 'lineItemId'.
   *
   * @param {string} lineItemId The ID of the line item to 'get'
   * @return {!LineItem} An object representing the retrieved line item resource
   */
  get(lineItemId) {
    return /** @type {!LineItem} */ (super.getResource(
        `advertisers/${this.getAdvertiserId()}/lineItems/${lineItemId}`));
  }

  /**
   * Creates a new line item resource based on the given 'lineItemResource'
   * object.
   *
   * @param {!DisplayVideoResource} lineItemResource The line item resource to
   *     create
   * @return {!LineItem} An object representing the created line item resource
   */
  create(lineItemResource) {
    return /** @type {!LineItem} */ (super.createResource(
        `advertisers/${this.getAdvertiserId()}/lineItems`, lineItemResource));
  }

  /**
   * Modifies a line item resource identified by 'lineItemId' based on the given
   * 'changedProperties'.
   *
   * @param {!DisplayVideoResource} lineItemResource The line item resource to
   *     'patch'
   * @param {string} changedProperties A comma-separated list of properties that
   *     have changed in the line item resource and therefore need updating
   * @return {!LineItem} An object representing the modified line item resource
   */
  patch(lineItemResource, changedProperties) {
    return /** @type {!LineItem} */ (super.patchResource(
        `advertisers/${this.getAdvertiserId()}/lineItems/` +
            `${lineItemResource.getId()}?updateMask=` +
            encodeURIComponent(changedProperties),
        lineItemResource));
  }

  /**
   * Deletes a line item resource identified by 'lineItemId'.
   *
   * @param {string} lineItemId The ID of the line item to 'delete'
   */
  delete(lineItemId) {
    super.deleteResource(
        `advertisers/${this.getAdvertiserId()}/lineItems/${lineItemId}`);
  }

  /**
   * Returns the DV360 Advertiser identifier.
   *
   * @return {string}
   */
  getAdvertiserId() {
    return this.advertiserId_;
  }
}

/**
 * An extension of `DisplayVideoApiClient` to handle {@link InventorySource}
 * resources.
 * @final
 */
class InventorySources extends DisplayVideoApiClient {
  /**
   * Constructs an instance of `InventorySources`.
   *
   * @param {string} partnerId The DV360 Partner identifier to fetch inventory
   *     sources for
   * @param {?string=} advertiserId Optional DV360 Advertiser identifier. If
   *     provided will be used for fetching inventory sources instead of the
   *     'partnerId'
   */
  constructor(partnerId, advertiserId = null) {
    super('inventorySources');

    /** @private @const {string} */
    this.partnerId_ = partnerId;

    /** @private @const {?string} */
    this.advertiserId_ = advertiserId;
  }

  /**
   * Retrieves all inventory source resources from the API, filtering them using
   * the given 'filter' and calling the given 'callback' for every retrieved
   * 'page' of data.
   *
   * @param {function(!Array<!InventorySource>): undefined} callback Callback to
   *     trigger after fetching every 'page' of inventory sources
   * @param {?FilterExpression=} filter Optional filter for filtering retrieved
   *     results. Defaults to filtering for 'active' inventory source resources
   * @param {number=} maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   */
  list(callback, filter = activeEntityFilter(), maxPages = -1) {
    const [key, value] = this.getAdvertiserId() ?
        ['advertiserId', this.getAdvertiserId()] :
        ['partnerId', this.getPartnerId()];
    const filterQueryString =
        filter ? `&filter=${filter.toApiQueryString()}` : '';
    super.listResources(
        `inventorySources?${key}=${value}${filterQueryString}`, callback,
        maxPages);
  }

  /**
   * Converts an inventory source resource object returned by the API into a
   * concrete {@link InventorySource} instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!InventorySource} The concrete inventory source instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   * @override
   */
  asDisplayVideoResource(resource) {
    return InventorySource.fromApiResource(resource);
  }

  /**
   * Retrieves a single inventory source from the API, identified by
   * 'inventorySourceId'.
   *
   * @param {string} inventorySourceId The ID of the inventory source to 'get'
   * @return {!InventorySource} An object representing the retrieved inventory
   *     source resource
   */
  get(inventorySourceId) {
    return /** @type {!InventorySource} */ (super.getResource(
        `inventorySources/${inventorySourceId}` +
        `?partnerId=${this.getPartnerId()}`));
  }

  /**
   * @param {string} requestUri
   * @param {!DisplayVideoResource} payload
   * @return {!DisplayVideoResource}
   * @throws {!Error} As this method is not allowed for this type
   * @override
   */
  createResource(requestUri, payload) {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * @param {string} requestUri
   * @return {!DisplayVideoResource}
   * @throws {!Error} As this method is not allowed for this type
   * @override
   */
  patchResource(requestUri) {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * @param {string} requestUri
   * @param {!DisplayVideoResource} original
   * @param {?DisplayVideoResource} modified
   * @return {!DisplayVideoResource}
   * @throws {!Error} As this method is not allowed for this type
   * @override
   */
  patchResourceByComparison(requestUri, original, modified) {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * @param {string} requestUri
   * @throws {!Error} As this method is not allowed for this type
   * @override
   */
  deleteResource(requestUri) {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * Returns the DV360 Partner identifier.
   *
   * @return {string}
   */
  getPartnerId() {
    return this.partnerId_;
  }

  /**
   * Returns the optional DV360 Advertiser identifier.
   *
   * @return {?string}
   */
  getAdvertiserId() {
    return this.advertiserId_;
  }
}

/**
 * An extension of `DisplayVideoApiClient` to handle {@link TargetingOption}
 * resources.
 * @final
 */
class TargetingOptions extends DisplayVideoApiClient {
  /**
   * Constructs an instance of `TargetingOptions`.
   *
   * @param {!TargetingType} targetingType The targeting type for retrieving
   *     targeting options
   * @param {string} advertiserId The DV360 advertiser identifier to use for
   *     retrieving targeting options
   */
  constructor(targetingType, advertiserId) {
    super('targetingOptions');

    /** @private @const {!TargetingType} */
    this.targetingType_ = targetingType;

    /** @private @const {string} */
    this.advertiserId_ = advertiserId;
  }

  /**
   * Retrieves all targeting option resources from the API, filtering them using
   * the given 'filter' and calling the given 'callback' for every retrieved
   * 'page' of data.
   *
   * @param {function(!Array<!TargetingOption>): undefined} callback Callback to
   *     trigger after fetching every 'page' of targeting options
   * @param {?FilterExpression=} filter Optional filter for filtering retrieved
   *     results. Defaults to null
   * @param {number=} maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   */
  list(callback, filter = null, maxPages = -1) {
    const filterQueryString =
        filter ? `&filter=${filter.toApiQueryString()}` : '';
    super.listResources(
        `targetingTypes/${this.getTargetingType()}/targetingOptions` +
            `?advertiserId=${this.getAdvertiserId()}${filterQueryString}`,
        callback, maxPages);
  }

  /**
   * Searches the API for targeting options of a given type based on the given
   * search query and calls the given 'callback' for every retrieved 'page' of
   * data. Only {@link TargetingType.GEO_REGION} is supported. The API
   * explicitly provides this method as 'filter' values for the 'list' method
   * for targeting options only support the 'EQ' rule operator, and search
   * queries do not have to be an exact match (e.g. a search query of "New "
   * would yield both "New Jersey" and "New York").
   *
   * @param {string} query The search query
   * @param {function(!Array<!TargetingOption>): undefined} callback Callback to
   *     trigger after fetching every 'page' of targeting options
   * @param {number=} maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   * @throws {!Error} If the targeting type is not GEO_REGION
   */
  search(query, callback, maxPages = -1) {
    if (this.getTargetingType() !== TargetingType.GEO_REGION) {
      throw new Error(
          `Error! "search" is only supported for ${TargetingType.GEO_REGION}`);
    }
    const url = `targetingTypes/${this.getTargetingType()}/` +
        encodeURIComponent('targetingOptions:search');
    const payload = {
      advertiserId: this.getAdvertiserId(),
      geoRegionSearchTerms: {
        geoRegionQuery: query,
      },
    };
    const params = {
      method: 'post',
      payload: JSON.stringify(payload),
    };
    super.listResources(url, callback, maxPages, params);
  }

  /**
   * Converts a targeting option resource object returned by the API into a
   * concrete {@link TargetingOption} instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!TargetingOption} The concrete targeting option instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   * @override
   */
  asDisplayVideoResource(resource) {
    return TargetingOption.fromApiResource(resource);
  }

  /**
   * Retrieves a single targeting option from the API, identified by
   * 'targetingOptionId'.
   *
   * @param {string} targetingOptionId The ID of the targeting option to 'get'
   * @return {!TargetingOption} An object representing the retrieved targeting
   *     option resource
   */
  get(targetingOptionId) {
    return /** @type {!TargetingOption} */ (super.getResource(
        `targetingTypes/${this.getTargetingType()}/targetingOptions/` +
        `${targetingOptionId}?advertiserId=${this.getAdvertiserId()}`));
  }

  /**
   * @param {string} requestUri
   * @param {!DisplayVideoResource} payload
   * @return {!DisplayVideoResource}
   * @throws {!Error} As this method is not allowed for this type
   * @override
   */
  createResource(requestUri, payload) {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * @param {string} requestUri
   * @return {!DisplayVideoResource}
   * @throws {!Error} As this method is not allowed for this type
   * @override
   */
  patchResource(requestUri) {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * @param {string} requestUri
   * @param {!DisplayVideoResource} original
   * @param {?DisplayVideoResource} modified
   * @return {!DisplayVideoResource}
   * @throws {!Error} As this method is not allowed for this type
   * @override
   */
  patchResourceByComparison(requestUri, original, modified) {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * @param {string} requestUri
   * @throws {!Error} As this method is not allowed for this type
   * @override
   */
  deleteResource(requestUri) {
    throw new Error('405 Method Not Allowed');
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
   * Returns the DV360 Advertiser identifier.
   *
   * @return {string}
   */
  getAdvertiserId() {
    return this.advertiserId_;
  }
}

/**
 * An extension of `DisplayVideoApiClient` to handle
 * {@link AssignedTargetingOption} resources.
 * @final
 */
class AssignedTargetingOptions extends DisplayVideoApiClient {
  /**
   * Constructs an instance of `AssignedTargetingOptions`.
   *
   * @param {!TargetingType} targetingType The targeting type for retrieving
   *     targeting options
   * @param {string} advertiserId The DV360 advertiser identifier to use for
   *     retrieving targeting options
   * @param {{
   *     campaignId: (?string|undefined),
   *     insertionOrderId: (?string|undefined),
   *     lineItemId: (?string|undefined),
   * }=} params
   */
  constructor(targetingType, advertiserId, {
        campaignId = null,
        insertionOrderId = null,
        lineItemId = null,
      } = {}) {
    super('assignedTargetingOptions');

    /** @private @const {!TargetingType} */
    this.targetingType_ = targetingType;

    /** @private @const {string} */
    this.advertiserId_ = advertiserId;

    /** @private @const {?string} */
    this.campaignId_ = campaignId;

    /** @private @const {?string} */
    this.insertionOrderId_ = insertionOrderId;

    /** @private @const {?string} */
    this.lineItemId_ = lineItemId;

    /**
     * Assigned targeting options are read-only (list & get operations only) for
     * campaigns and insertion orders.
     * @private @const {boolean}
     */
    this.readOnly_ = campaignId != null || insertionOrderId != null;
  }

  /**
   * Returns the base url string for every API operation. Checks the initialized
   * constructor parameters and adds the necessary extensions to the resulting
   * string.
   *
   * @return {string} The base url for every API operation
   */
  getBaseUrl() {
    const prefix = `advertisers/${this.getAdvertiserId()}/`;
    const suffix = `targetingTypes/${this.getTargetingType()}/` +
        `assignedTargetingOptions`;
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
   * @param {function(!Array<!AssignedTargetingOption>): undefined} callback
   *     Callback to trigger after fetching every 'page' of assigned targeting
   *     options
   * @param {?FilterExpression=} filter Optional filter for filtering retrieved
   *     results. Defaults to null
   * @param {number=} maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   */
  list(callback, filter = null, maxPages = -1) {
    const filterQueryString =
        filter ? `?filter=${filter.toApiQueryString()}` : '';
    super.listResources(
        this.getBaseUrl() + filterQueryString, callback, maxPages);
  }

  /**
   * Converts an assigned targeting option resource object returned by the API
   * into a concrete {@link AssignedTargetingOption} instance.
   *
   * @param {!Object<string, *>} resource The API resource object
   * @return {!AssignedTargetingOption} The concrete assigned targeting option
   *     instance
   * @throws {!Error} If the API resource object did not contain the expected
   *     properties
   * @override
   */
  asDisplayVideoResource(resource) {
    return AssignedTargetingOption.fromApiResource(resource);
  }

  /**
   * Retrieves a single assigned targeting option from the API, identified by
   * 'assignedTargetingOptionId'.
   *
   * @param {string} assignedTargetingOptionId The ID of the assigned targeting
   *     option to 'get'
   * @return {!AssignedTargetingOption} An object representing the retrieved
   *     assigned targeting option resource
   */
  get(assignedTargetingOptionId) {
    return /** @type {!AssignedTargetingOption} */ (
        super.getResource(`${this.getBaseUrl()}/${assignedTargetingOptionId}`));
  }

  /**
   * Creates a new assigned targeting option resource based on the given
   * 'assignedTargetingOptionResource' object.
   *
   * @param {!DisplayVideoResource} assignedTargetingOptionResource The
   *     assigned targeting option resource to create
   * @return {!AssignedTargetingOption} An object representing the created
   *     assigned targeting option resource
   * @throws {!Error} If this method is not allowed for this type
   */
  create(assignedTargetingOptionResource) {
    return /** @type {!AssignedTargetingOption} */ (this.createResource(
        this.getBaseUrl(), assignedTargetingOptionResource));
  }

  /**
   * @param {string} requestUri
   * @param {!DisplayVideoResource} payload
   * @return {!DisplayVideoResource}
   * @throws {!Error} If this method is not allowed for this type
   * @override
   */
  createResource(requestUri, payload) {
    if (this.isReadOnly()) {
      throw new Error('405 Method Not Allowed');
    }
    return super.createResource(requestUri, payload);
  }

  /**
   * @param {string} requestUri
   * @return {!DisplayVideoResource}
   * @throws {!Error} As this method is not allowed for this type
   * @override
   */
  patchResource(requestUri) {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * @param {string} requestUri
   * @param {!DisplayVideoResource} original
   * @param {?DisplayVideoResource} modified
   * @return {!DisplayVideoResource}
   * @throws {!Error} As this method is not allowed for this type
   * @override
   */
  patchResourceByComparison(requestUri, original, modified) {
    throw new Error('405 Method Not Allowed');
  }

  /**
   * Deletes an assigned targeting option identified by
   * 'assignedTargetingOptionId'.
   *
   * @param {string} assignedTargetingOptionId The ID of the assigned targeting
   *     option to 'delete'
   */
  delete(assignedTargetingOptionId) {
    this.deleteResource(`${this.getBaseUrl()}/${assignedTargetingOptionId}`);
  }

  /**
   * @param {string} requestUri
   * @throws {!Error} If this method is not allowed for this type
   * @override
   */
  deleteResource(requestUri) {
    if (this.isReadOnly()) {
      throw new Error('405 Method Not Allowed');
    }
    super.deleteResource(requestUri);
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
   * Returns the DV360 advertiser identifier.
   *
   * @return {string}
   */
  getAdvertiserId() {
    return this.advertiserId_;
  }

  /**
   * Returns the DV360 campaign identifier.
   *
   * @return {?string}
   */
  getCampaignId() {
    return this.campaignId_;
  }

  /**
   * Returns the DV360 insertion order identifier.
   *
   * @return {?string}
   */
  getInsertionOrderId() {
    return this.insertionOrderId_;
  }

  /**
   * Returns the DV360 line item identifier.
   *
   * @return {?string}
   */
  getLineItemId() {
    return this.lineItemId_;
  }

  /**
   * Whether this API client is read only (only list and get operations are
   * supported) or not.
   *
   * @return {boolean}
   */
  isReadOnly() {
    return this.readOnly_;
  }
}
