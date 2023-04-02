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

import {ObjectUtil, UriUtil} from './utils';

/**
 * DV360 API resources contain page tokens.
 */
export interface PagedDisplayVideoResponse {
  pageToken: string;
}

interface Params {
  [key: string]: string | Params;
}

/**
 * Base class encapsulating all logic to access any Google API using the
 * built-in Google Apps Script service {@link UrlFetchApp}.
 *
 * @see appsscript.json for a list of enabled advanced services and API scopes.
 */
export class BaseApiClient {
  /**
   * Constructs an instance of BaseApiClient.
   *
   * @param apiScope The API scope
   * @param apiVersion The API version
   */
  constructor(private readonly apiScope: string, private readonly apiVersion: string) {
  }

  /**
   * Executes a paged API request (e.g. GET with pageToken). Keeps track of
   * paged responses and delegates to @link {executeApiRequest} for the concrete
   * request and response handling. Accepts a callback which is used to output
   * intermediate results while fetching more pages.
   *
   * @param requestUri The URI of the GET request
   * @param requestParams The options to use for the
   *     GET request
   * @param requestCallback The method
   *     to call after the request has executed successfully
   * @param maxPages The max number of pages to fetch. Defaults to -1
   *     indicating 'fetch all'
   */
  executePagedApiRequest(
    requestUri: string,
    requestParams: {[key: string]: string} | null,
    requestCallback: (p1: {[key: string]: unknown}) => void,
    maxPages: number = -1
  ) {
    let url = this.buildApiUrl(requestUri);
    let pageCount = 1;
    let pageToken;

    do {
      const result = this.executeApiRequest(url, requestParams, true);
      console.log(`Output results page: ${pageCount}`);
      requestCallback(result);

      pageToken = result.nextPageToken;

      if (pageToken) {
        if (requestParams && requestParams['payload']) {
          const payload = JSON.parse(
            String(requestParams['payload'])
          ) as PagedDisplayVideoResponse;
          payload.pageToken = pageToken;
          requestParams['payload'] = JSON.stringify(payload);
        } else {
          url = UriUtil.modifyUrlQueryString(url, 'pageToken', pageToken);
        }
      }
      pageCount++;
    } while (pageToken && (maxPages < 0 || pageCount <= maxPages));
  }

  /**
   * Executes a request to the API while handling errors and response
   * data parsing. Re-attempts failed executions up to the value of
   * 'maxRetries'.
   *
   * @param requestUri The URI of the request
   * @param requestParams The options to use for the
   *     request
   * @param retryOnFailure Whether the operation should be retried
   *     in case of failure or not
   * @param operationCount The number of failed attempts made.
   * @return The parsed JSON response
   *     data, or an empty object for empty responses
   */
  executeApiRequest(
    requestUri: string,
    requestParams: Params | null,
    retryOnFailure: boolean,
    operationCount: number = 0
  ): {nextPageToken?: string} {
    const url = this.buildApiUrl(requestUri);
    const params = this.buildApiParams(requestParams);
    const maxRetries = 3;

    try {
      console.log(`Fetching url=${url} with params=${JSON.stringify(params)}`);
      const response = UrlFetchApp.fetch(url, params);
      const result = response.getContentText()
        ? (JSON.parse(response.getContentText()) as PagedDisplayVideoResponse)
        : {};
      return result;
    } catch (e) {
      console.error(`Operation failed with exception: ${e}`);

      if (retryOnFailure && operationCount < maxRetries) {
        console.info(`Retrying operation for a max of ${maxRetries} times...`);
        operationCount++;
        return this.executeApiRequest(
          url,
          params,
          retryOnFailure,
          operationCount
        );
      } else {
        console.warn(
          'Retry on failure not supported or all retries ' +
            'have been exhausted... Failing!'
        );
        throw e;
      }
    }
  }

  /**
   * Constructs the fully-qualified API URL using the given requestUri if not
   * already done.
   *
   * @param requestUri The URI of the request
   * @return The fully-qualified API URL
   */
  buildApiUrl(requestUri: string): string {
    const protocolAndDomain = `https://${this.apiScope}.googleapis.com/`;

    if (requestUri.startsWith(protocolAndDomain)) {
      return requestUri;
    }
    return `${protocolAndDomain}${this.apiVersion}/${requestUri}`;
  }

  /**
   * Constructs the options to use for API requests, extending default options
   * provided by the given requestParams.
   *
   * @param requestParams The options to use for the
   *     request
   * @return The extended request options to use
   */
  buildApiParams(requestParams: Params | null): Params {
    const token = ScriptApp.getOAuthToken();
    const baseParams: Params = {
      'contentType': 'application/json',
      'headers': {Authorization: `Bearer ${token}`, Accept: 'application/json'},
    };
    return ObjectUtil.extend(baseParams, requestParams || {});
  }

  /**
   * Returns the API scope.
   *
   * @return The API scope
   */
  getApiScope(): string {
    return this.apiScope;
  }

  /**
   * Returns the API version.
   *
   * @return The API version
   */
  getApiVersion(): string {
    return this.apiVersion;
  }
}
