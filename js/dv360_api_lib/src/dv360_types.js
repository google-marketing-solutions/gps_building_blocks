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
      return /** @type {!AdvertiserGeneralConfig} */(resource);
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
        return /** @type {!AdvertiserAdServerConfig} */(resource);
      }
    }
    return null;
  },
};

