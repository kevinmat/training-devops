package onevz.vxp.serviceordermanagement.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.vzw.cxp.domainservices.vo.DomainServicesResponse;

import lombok.extern.slf4j.Slf4j;
import onevz.vxp.serviceordermanagement.audit.model.AuditMessage;
import onevz.vxp.serviceordermanagement.audit.model.AuditValidationReport;
import onevz.vxp.serviceordermanagement.audit.model.AuditValidationRequest;
import onevz.vxp.serviceordermanagement.audit.model.AuditValidationRequestData;
import onevz.vxp.serviceordermanagement.audit.model.AuditValidationResponse;
import onevz.vxp.serviceordermanagement.audit.model.AuditValidationResponseData;
import onevz.vxp.serviceordermanagement.audit.model.BIBucket;
import onevz.vxp.serviceordermanagement.audit.model.BIFeature;
import onevz.vxp.serviceordermanagement.audit.model.FailureData;
import onevz.vxp.serviceordermanagement.audit.model.FailureType;
import onevz.vxp.serviceordermanagement.audit.model.SIBucket;
import onevz.vxp.serviceordermanagement.audit.model.SIFeature;
import onevz.vxp.serviceordermanagement.audit.model.SIFeatureCharacteristic;
import onevz.vxp.serviceordermanagement.audit.model.Status;
import onevz.vxp.serviceordermanagement.domain.model.BucketDTO;
import onevz.vxp.serviceordermanagement.domain.model.FeatureSpecificationCharacteristicDTO;
/*import onevz.vxp.serviceordermanagement.domain.model.ReferenceDataResponse;
import onevz.vxp.serviceordermanagement.domain.model.*;
import onevz.vxp.serviceordermanagement.domain.model.ServiceOrderDTO;
import onevz.vxp.serviceordermanagement.domain.model.ServiceOrderGetResponse;*/
import onevz.vxp.serviceordermanagement.domain.model.ReferenceDataResponse;
import onevz.vxp.serviceordermanagement.domain.model.RfsFeaturesDTO;
import onevz.vxp.serviceordermanagement.domain.model.ServiceOrderDTO;
import onevz.vxp.serviceordermanagement.domain.model.ServiceOrderGetResponse;
import onevz.vxp.serviceordermanagement.domain.model.ServiceOrderUpdateResponse;
import onevz.vxp.serviceordermanagement.configuration.AuditValidationProperties;
import onevz.vxp.serviceordermanagement.service.AuditValidationService;
import onevz.vxp.serviceordermanagement.service.ProvisioningDataService;
import onevz.vxp.serviceordermanagement.service.ServiceOrderService;
import onevz.vxp.serviceordermanagement.utils.constants.AppConstants;
import onevz.vxp.serviceordermanagement.helper.FeatureFlagHelper;
import reactor.core.publisher.Mono;

@Slf4j
@Service
public class AuditValidationServiceImpl implements AuditValidationService {

	private final ServiceOrderService serviceOrderService;
	
	private final ProvisioningDataService provisioningDataServiceImpl;
	
	private final FeatureFlagHelper featureFlagHelper;

	private final AuditValidationProperties auditValidationProperties;

	public AuditValidationServiceImpl(ServiceOrderService serviceOrderService, ProvisioningDataService provisioningDataServiceImpl,
			FeatureFlagHelper featureFlagHelper, AuditValidationProperties auditValidationProperties) {
		this.serviceOrderService = serviceOrderService;
		this.provisioningDataServiceImpl = provisioningDataServiceImpl;
		this.featureFlagHelper = featureFlagHelper;
		this.auditValidationProperties = auditValidationProperties;
	}

	@Override
	public Mono<DomainServicesResponse<AuditValidationResponse>> performAuditValidation(
			AuditValidationRequest auditValidationRequest) {
		
		log.info("In AuditValidationServiceImpl.performAuditValidation() :  {}", auditValidationRequest);
		
		Mono<ReferenceDataResponse> referenceDataResponseMono = provisioningDataServiceImpl.fetchProvisioningDataById(AppConstants.BUNDLE_CONFIG)
				.map(DomainServicesResponse::getData);

		Mono<ServiceOrderGetResponse> serviceOrderGetResponseMono = serviceOrderService.getServiceOrderDetails(auditValidationRequest.getData().getServiceOrderId())
				.map(DomainServicesResponse::getData);

		return Mono.zip(referenceDataResponseMono, serviceOrderGetResponseMono, Mono.just(auditValidationRequest))
				.flatMap(tuple -> {
					ReferenceDataResponse referenceDataResponse = tuple.getT1();
					ServiceOrderGetResponse serviceOrderGetResponse = tuple.getT2();
					AuditValidationRequest auditRequest = tuple.getT3();
					return performValidation(referenceDataResponse, serviceOrderGetResponse, auditRequest);
				});

	}

	private Mono<DomainServicesResponse<AuditValidationResponse>> performValidation(
			ReferenceDataResponse referenceDataResponse, ServiceOrderGetResponse serviceOrderGetResponse,
			AuditValidationRequest request) {
		
		log.info("In AuditValidationServiceImpl.performValidation() :  {}", referenceDataResponse,serviceOrderGetResponse,request);
		
		AuditValidationResponse auditValidationResponse = new AuditValidationResponse();
		AuditValidationResponseData responseData = new AuditValidationResponseData();
		AuditValidationReport auditValidationReport = new AuditValidationReport();
		List<AuditMessage> auditMessages = new ArrayList<>();
		AuditValidationRequestData requestData = request.getData();

		
		if (requestData == null || serviceOrderGetResponse.getServiceOrderDataModel() == null ) {

			return processDomainServiceResponse(requestData,responseData,auditValidationResponse);
		}

		ServiceOrderDTO serviceOrderDTO = serviceOrderGetResponse.getServiceOrderDataModel();
        requestData.setServiceOrderDataModel(serviceOrderDTO);
		Map<String, String> ocsBundleMapping = extractOcsBundleMapping(referenceDataResponse);

        if (featureFlagHelper.isAuditFeatureSkipFFlag()) {
            performFeatureComparison(serviceOrderDTO.getRfsFeatures(), requestData.getSiFeatures(), ocsBundleMapping, auditMessages);
            performBucketComparison(requestData.getBiBuckets(), requestData.getSiBuckets(), serviceOrderDTO.getBuckets(),
                    auditMessages, requestData);
            performBundleComparison(serviceOrderDTO.getRfsFeatures(), requestData.getSiFeatures(), requestData.getBiFeatures(), requestData.getSiBuckets(), ocsBundleMapping, auditMessages);
        }
		performServiceEndDateComparison(serviceOrderDTO.getServiceEndDate(), requestData.getSiServiceEndDate(),
				requestData.getBiServiceEndDate(), auditMessages);
		performIccidComparison(requestData.getQmdnIccid(), requestData.getSimSerialNumber(), auditMessages);
        performMdnStatusCheck(requestData, auditMessages);
		log.debug("auditMessages: {}", auditMessages);
		auditValidationReport.setAuditReport(buildAuditReportWithAllTiles(auditMessages));

		if (auditMessages.isEmpty()) {
			responseData = processAuditValidationResponseData(AppConstants.PASS,new Status(AppConstants.SUCCESS_STATUS_CODE, AppConstants.SUCCESS));
		} else {
			responseData = processAuditValidationResponseData(AppConstants.FAIL,new Status(AppConstants.SUCCESS_STATUS_CODE, AppConstants.SUCCESS));
		}
		
		
		createAuditStatusRequest(auditValidationReport.getAuditReport(),responseData.getAuditStatus(),serviceOrderDTO,serviceOrderGetResponse);	
		Mono<DomainServicesResponse<ServiceOrderUpdateResponse>> auditStatusUpdate = perfromAuditStatusUpdate(serviceOrderDTO);
		auditValidationResponse.setAuditResponse(responseData);
		
		if (auditStatusUpdate != null) {
			log.info("Audit Response after status update: {}", auditValidationResponse);
		    return auditStatusUpdate.doOnNext(updateRes -> log.info(AppConstants.AUDIT_UPDATE_MSG))
		        .map(updateRes -> convertAuditResponse(auditValidationResponse));

		} else {
			log.info("Audit Response : {}", auditValidationResponse);
		    return Mono.just(convertAuditResponse(auditValidationResponse));
		}
		
	}

    private void performMdnStatusCheck(AuditValidationRequestData requestData, List<AuditMessage> auditMessages) {
        // Early exit if prerequisites not met
        if (!isMdnStatusCheckRequired(requestData)) {
            return;
        }

        // Skip for FWA devices if feature flag is enabled
        if (shouldSkipFwaDevice(requestData)) {
            log.info("⊘ MDN Status Check Skipped: Skipped for FWA device - FwaNodeId: {}, FwaReservationId: {}",
                    requestData.getFwaNodeId(), requestData.getFwaReservationId());
            return;
        }

        String omdnStatus = requestData.getQmdnData().getOmdnStatus();
        String expectedStatus = getExpectedStatus(omdnStatus);

        if (expectedStatus == null) {
            return;
        }

        String serviceOrderType = Optional.ofNullable(requestData.getServiceOrderDataModel())
                .map(ServiceOrderDTO::getServiceOrderType)
                .orElse(null);

        // Handle special order types or default validation
        handleMdnStatusValidation(requestData, auditMessages, omdnStatus, expectedStatus, serviceOrderType);
    }

    private boolean isMdnStatusCheckRequired(AuditValidationRequestData requestData) {
        return Optional.ofNullable(requestData)
                .map(AuditValidationRequestData::getQmdnData)
                .map(onevz.vxp.serviceordermanagement.domain.model.QmdnData::getOmdnStatus)
                .filter(status -> !status.isEmpty())
                .isPresent();
    }

    private boolean shouldSkipFwaDevice(AuditValidationRequestData requestData) {
        return featureFlagHelper.isFwaSkipAuditChecksFFlag() && isFwaDevice(requestData);
    }

    private String getExpectedStatus(String omdnStatus) {
        Map<String, String> omdnStatusMapping = auditValidationProperties.getOmdnstatus();

        if (omdnStatusMapping == null || omdnStatusMapping.isEmpty()) {
            log.warn("OMDN status mapping configuration is empty or null");
            return null;
        }

        String expectedStatus = omdnStatusMapping.get(omdnStatus);
        if (expectedStatus == null) {
            log.warn("OMDN status '{}' not found in configuration mapping", omdnStatus);
        }
        return expectedStatus;
    }

    private void handleMdnStatusValidation(AuditValidationRequestData requestData, List<AuditMessage> auditMessages,
                                           String omdnStatus, String expectedStatus, String serviceOrderType) {
        String siStatus = requestData.getSiStatus();
        String biLifeCycleState = requestData.getBiLifeCycleState();

        if ("SUSPEND".equalsIgnoreCase(serviceOrderType)) {
            validateSuspendOrder(auditMessages, omdnStatus, siStatus, biLifeCycleState, expectedStatus);
        } else if ("HOTLINE".equalsIgnoreCase(serviceOrderType)) {
            validateHotlineOrder(auditMessages, omdnStatus, siStatus, biLifeCycleState, expectedStatus);
        } else {
            validateDefaultOrder(auditMessages, omdnStatus, expectedStatus, siStatus, biLifeCycleState);
        }
    }

    private void validateSuspendOrder(List<AuditMessage> auditMessages, String omdnStatus, String siStatus,
                                      String biLifeCycleState, String expectedStatus) {
        boolean isValid = "EXPIRED".equalsIgnoreCase(biLifeCycleState)
                && "SUSPEND".equalsIgnoreCase(siStatus)
                && "ACTIVE".equalsIgnoreCase(expectedStatus);

        if (!isValid) {
            String errorMessage = String.format(
                    "MDN status Mismatch for SUSPEND order. Expected - OCS:EXPIRED, SI:SUSPEND, QMDN:%s . Actual - QMDN status:%s, SI siStatus:%s, OCS status:%s",
                    omdnStatus, omdnStatus, siStatus, biLifeCycleState
            );

            auditMessages.add(createAuditMessage("MDN", "", errorMessage,
                    "OCS:EXPIRED, SI:SUSPEND, QMDN:ACTIVE",
                    String.format("OCS:%s, SI:%s, QMDN:%s", biLifeCycleState, siStatus, expectedStatus)));
            log.error(errorMessage);
        }
    }

    private void validateHotlineOrder(List<AuditMessage> auditMessages, String omdnStatus, String siStatus,
                                      String biLifeCycleState, String expectedStatus) {
        boolean isValid = "ACTIVE".equalsIgnoreCase(biLifeCycleState)
                && "HOTLINE".equalsIgnoreCase(siStatus)
                && "ACTIVE".equalsIgnoreCase(expectedStatus);

        if (!isValid) {
            String errorMessage = String.format(
                    "MDN status Mismatch for HOTLINE order. Expected - OCS:ACTIVE, SI:HOTLINE, QMDN:%s (mapped to ACTIVE). Actual - QMDN status:%s, SI siStatus:%s, OCS status:%s",
                    omdnStatus, omdnStatus, siStatus, biLifeCycleState
            );

            auditMessages.add(createAuditMessage("MDN", "", errorMessage,
                    "OCS:ACTIVE, SI:HOTLINE, QMDN:ACTIVE",
                    String.format("OCS:%s, SI:%s, QMDN:%s", biLifeCycleState, siStatus, expectedStatus)));
            log.error(errorMessage);
        }
    }

    private void validateDefaultOrder(List<AuditMessage> auditMessages, String omdnStatus, String expectedStatus,
                                      String siStatus, String biLifeCycleState) {
        boolean siMatches = Optional.ofNullable(siStatus)
                .map(status -> status.equalsIgnoreCase(expectedStatus))
                .orElse(false);

        boolean biMatches = Optional.ofNullable(biLifeCycleState)
                .map(status -> status.equalsIgnoreCase(expectedStatus))
                .orElse(false);

        if (!siMatches || !biMatches) {
            String errorMessage = String.format(
                    "MDN status Mismatch. QMDN status:%s (mapped to %s), SI siStatus:%s, OCS status:%s",
                    omdnStatus, expectedStatus, siStatus, biLifeCycleState
            );

            auditMessages.add(createAuditMessage("MDN", "", errorMessage, expectedStatus,
                    String.format("SI:%s, OCS:%s", siStatus, biLifeCycleState)));
            log.error(errorMessage);
        }
    }

    private DomainServicesResponse<AuditValidationResponse> convertAuditResponse(AuditValidationResponse data) {
	    DomainServicesResponse<AuditValidationResponse> response = new DomainServicesResponse<>();
	    response.setData(data);
	    return response;
	}

	private Mono<DomainServicesResponse<ServiceOrderUpdateResponse>> perfromAuditStatusUpdate(
			ServiceOrderDTO serviceOrderDTO) {
		return serviceOrderService.updateServiceOrder(serviceOrderDTO);
	}

	private AuditValidationResponseData processAuditValidationResponseData(String auditStatus, Status status) {
		AuditValidationResponseData auditValidationResponseData = new AuditValidationResponseData();
		auditValidationResponseData.setAuditStatus(auditStatus);
		auditValidationResponseData.setStatus(status);
		return auditValidationResponseData;
	}

	private Mono<DomainServicesResponse<AuditValidationResponse>> processDomainServiceResponse(AuditValidationRequestData requestData, AuditValidationResponseData responseData,
			AuditValidationResponse auditValidationResponse) {
		responseData.setAuditStatus(AppConstants.FAIL);
		responseData.setStatus(new Status(AppConstants.INVALID_REQUEST_CODE, AppConstants.INVALID_REQ_MSG));
		auditValidationResponse.setAuditResponse(responseData);
		DomainServicesResponse<AuditValidationResponse> domainServicesResponse = new DomainServicesResponse<>();
		domainServicesResponse.setData(auditValidationResponse);
		return Mono.just(domainServicesResponse);

	}

	private void createAuditStatusRequest(List<FailureType> auditReport, String auditStatus, ServiceOrderDTO serviceOrderDTO,ServiceOrderGetResponse getResponse) {
		if(null != getResponse && null != getResponse.getServiceOrderDataModel()) {
			serviceOrderDTO.setAuditStatus(auditStatus);
			serviceOrderDTO.setAuditReport(auditReport);
		}
	}

	private Map<String, String> extractOcsBundleMapping(ReferenceDataResponse referenceDataResponse) {
		Map<String, String> ocsBundleMapping = new HashMap<>();
		try {
			if (referenceDataResponse != null && referenceDataResponse.getResults() != null
					&& referenceDataResponse.getResults().getRetrieveByKeyId() != null
					&& !referenceDataResponse.getResults().getRetrieveByKeyId().isEmpty()) {

				String bundleConfigJson = referenceDataResponse.getResults().getRetrieveByKeyId().get(0).getValue();
				String keyToFind = AppConstants.OCS_BUNDLE;
		        int startIndex = bundleConfigJson.indexOf(keyToFind);
				if (bundleConfigJson != null && !bundleConfigJson.isEmpty()) {
					if (startIndex != -1) {
						try {
							startIndex += keyToFind.length();
							int endIndex = bundleConfigJson.indexOf("}", startIndex);
							if (endIndex != -1) {
								String ocsBundleMappingValue = bundleConfigJson.substring(startIndex, endIndex + 1);
								ocsBundleMapping.put(AppConstants.OCS_BUNDLE_MAPPING,ocsBundleMappingValue);
							}
							
						} catch (Exception jsonEx) {
							log.info("JSON parsing failed, attempting Map toString() format parsing");
							parseMapStringFormat(bundleConfigJson, ocsBundleMapping);
						}
					} else {
						parseMapStringFormat(bundleConfigJson, ocsBundleMapping);
					}
				}
			}
		} catch (Exception e) {
			log.error("Error extracting ocsBundleMapping from reference data", e.getMessage());
		}
		return ocsBundleMapping;
	}
	
	private void parseMapStringFormat(String mapString, Map<String, String> ocsBundleMapping) {
		try {
			String searchKey = AppConstants.OCS_BUNDLE;
			int startIndex = mapString.indexOf(searchKey);
			
			if (startIndex != -1) {
				startIndex += searchKey.length();
				
				int braceCount = 0;
				int endIndex = startIndex;
				boolean started = false;
				
				for (int i = startIndex; i < mapString.length(); i++) {
					char c = mapString.charAt(i);
					if (c == '{') {
						braceCount++;
						started = true;
					} else if (c == '}') {
						braceCount--;
						if (started && braceCount == 0) {
							endIndex = i;
							break;
						}
					}
				}
				
				String mappingContent = mapString.substring(startIndex + 1, endIndex);
				
				String[] pairs = mappingContent.split(",\\s*(?![^{]*})");
				for (String pair : pairs) {
					String[] keyValue = pair.split("=", 2);
					if (keyValue.length == 2) {
						String key = keyValue[0].trim();
						String value = keyValue[1].trim();
						ocsBundleMapping.put(key, value);
						log.debug("Extracted mapping: {} -> {}", key, value);
					}
				}
			}
		} catch (Exception e) {
			log.error("Error parsing Map string format for ocsBundleMapping", e);
		}
	}

	private void performBucketComparison(List<BIBucket> biBuckets, List<SIBucket> siBuckets, List<BucketDTO> dcmBuckets,
			List<AuditMessage> auditMessages, AuditValidationRequestData requestData) {
		
		// Skip bucket comparison for FWA devices if feature flag is enabled
		if (featureFlagHelper.isFwaSkipAuditChecksFFlag() && isFwaDevice(requestData)) {
			log.info("⊘ Bucket Comparison Skipped: Skipped for FWA device - FwaNodeId: {}, FwaReservationId: {}", 
					requestData.getFwaNodeId(), requestData.getFwaReservationId());
			return;
		}
		
		List<BIBucket> validBiBuckets = biBuckets != null
				? biBuckets.stream().filter(b -> b.getId() != null).collect(Collectors.toList())
				: new ArrayList<>();

		Map<String, BIBucket> biBucketMap = validBiBuckets.stream()
				.collect(Collectors.toMap(BIBucket::getId, b -> b, (b1, b2) -> b1));
		Set<String> matchedBiBucketIds = new HashSet<>();

		if (siBuckets != null) {
			for (SIBucket siBucket : siBuckets) {
				String bucketName = siBucket.getBucketName();
				if(bucketName != null) {
					BIBucket matchingBiBucket = biBucketMap.get(bucketName);
	
					if (matchingBiBucket != null) {
						matchedBiBucketIds.add(matchingBiBucket.getId());
						compareBucketValues(siBucket, matchingBiBucket, auditMessages);
						compareBucketEndDates(siBucket, matchingBiBucket, auditMessages);
					} else {
						AuditMessage message = createAuditMessage("Bucket missing in OCS", bucketName,
								"Bucket missing is OCS:" + bucketName, bucketName, null);
						auditMessages.add(message);
					}
				}
			}
		}

		validBiBuckets.stream().filter(biBucket -> !matchedBiBucketIds.contains(biBucket.getId())).forEach(biBucket -> {
			log.info("Additional Bucket name in OCS: {}", biBucket.getId());
		});

		compareBucketPresenceAcrossDcmSiOcs(validBiBuckets, siBuckets, dcmBuckets, auditMessages);
	}

	private void compareBucketPresenceAcrossDcmSiOcs(List<BIBucket> biBuckets, List<SIBucket> siBuckets,
			List<BucketDTO> dcmBuckets, List<AuditMessage> auditMessages) {
		Set<String> ocsBucketNames = biBuckets == null ? new HashSet<>() : biBuckets.stream()
				.map(BIBucket::getId)
				.filter(Objects::nonNull)
				.collect(Collectors.toSet());

		Set<String> siBucketNames = siBuckets == null ? new HashSet<>() : siBuckets.stream()
				.map(SIBucket::getBucketName)
				.filter(Objects::nonNull)
				.collect(Collectors.toSet());

		Set<String> dcmBucketNames = dcmBuckets == null ? new HashSet<>() : dcmBuckets.stream()
				.map(BucketDTO::getBucketName)
				.filter(Objects::nonNull)
				.collect(Collectors.toSet());

		// Additional Buckets: OCS has extra buckets not present in both SI and DCM.
		ocsBucketNames.stream()
				.filter(ocsBucket -> !siBucketNames.contains(ocsBucket) && !dcmBucketNames.contains(ocsBucket))
				.forEach(ocsBucket -> auditMessages.add(createAuditMessage("Additional Buckets", ocsBucket,
						"Additional bucket in OCS compared to SI and DCM", null, ocsBucket)));

		// Missing Buckets: OCS or SI has fewer buckets than DCM.
		dcmBucketNames.stream()
				.filter(dcmBucket -> !ocsBucketNames.contains(dcmBucket))
				.forEach(dcmBucket -> auditMessages.add(createAuditMessage("Missing Buckets", dcmBucket,
						"Bucket from DCM missing in OCS", dcmBucket, null)));

		dcmBucketNames.stream()
				.filter(dcmBucket -> !siBucketNames.contains(dcmBucket))
				.forEach(dcmBucket -> auditMessages.add(createAuditMessage("Missing Buckets", dcmBucket,
						"Bucket from DCM missing in SI", dcmBucket, null)));
	}

	private void compareBucketValues(SIBucket siBucket, BIBucket biBucket, List<AuditMessage> auditMessages) {
		try {
			if(biBucket.getMeasUnit() == null) {
				//Buckets without 'Unit of measurement' are Unlimited bucket, No need to compare the values.
				log.debug("Skipping the unlimited bucket for comparison: {}", biBucket.getId());
				return;
			}
			
			double siBucketValue = parseDouble(siBucket.getBucketValue());
			double biBucketValue = biBucket.getCounterValue() != null ? biBucket.getCounterValue() : 0.0;

			double siValueInBaseUnit = convertToBaseUnit(siBucketValue, siBucket.getUnitOfMeasure());
			double biValueInBaseUnit = convertToBaseUnit(biBucketValue, biBucket.getMeasUnit());

			double difference = Math.abs(siValueInBaseUnit - biValueInBaseUnit);
			double bucketRelaxationInBaseUnit = convertToBaseUnit(AppConstants.BUCKET_VALUE_RELAXATION_MB, "mb");

			if (difference > bucketRelaxationInBaseUnit) {
				AuditMessage message = createAuditMessage("Bucket value", biBucket.getId(),
						String.format(
								"Bucket values mismatch beyond relaxation. BundleInquiry:%s, ServiceInventory:%s, AllowedRelaxation:%s MB",
								biBucket.getCounterValue(), siBucket.getBucketValue(), AppConstants.BUCKET_VALUE_RELAXATION_MB),
						String.valueOf(siBucketValue), String.valueOf(biBucketValue));
				auditMessages.add(message);
			}
		} catch (Exception e) {
			log.error("Error comparing bucket values for bucket: {}", siBucket.getBucketName(), e);
		}
	}

	private void compareBucketEndDates(SIBucket siBucket, BIBucket biBucket, List<AuditMessage> auditMessages) {
		try {
			String siExpiryDate = siBucket.getExpiryDate();
			Integer biEndDate = biBucket.getEndDate();

			if (siExpiryDate != null && biEndDate != null) {
				String normalizedSiDate = normalizeDateFormat(siExpiryDate);
				String normalizedBiDate = String.valueOf(biEndDate);

				if (!normalizedSiDate.equals(normalizedBiDate)) {
					AuditMessage message = createAuditMessage("Bucket end date", biBucket.getId(),
							String.format("Bucket end-date mismatch: BundleInquiry:%s, ServiceInventory:%s", biEndDate,
									siExpiryDate), normalizedSiDate, normalizedBiDate);
					auditMessages.add(message);
				}
			}
		} catch (Exception e) {
			log.error("Error comparing bucket end dates for bucket: {}", siBucket.getBucketName(), e);
		}
	}
	
	private void performFeatureComparison(List<RfsFeaturesDTO> list, List<SIFeature> siFeatures,
			Map<String, String> ocsBundleMapping, List<AuditMessage> auditMessages) {

		Set<String> siFeatureValues = extractSiFeatureValues(siFeatures);
		Set<String> rfsFeatureValues = extractRfsFeatureValues(list);

		// 1. Identify RFS features missing in SI
		rfsFeatureValues.forEach(rfsValue -> {
            if (rfsValue != null && rfsValue.endsWith(AppConstants.ROAMING_BUNDLE_FEATURE_TYPE)) {
                return;
            }
			if (!siFeatureValues.contains(rfsValue)) {
				auditMessages.add(createAuditMessage("Feature Missing", rfsValue,
						"Feature missing in the Service Inventory", rfsValue, null));
			}
		});

		// 2. Identify additional features in SI not in RFS
        siFeatureValues.stream()
                .filter(siValue -> !rfsFeatureValues.contains(siValue))
                .filter(siValue -> !(siValue != null && siValue.endsWith(AppConstants.ROAMING_BUNDLE_FEATURE_TYPE)))
                .forEach(siValue -> auditMessages.add(createAuditMessage("Additional Feature", siValue,
                        "Additional Feature in the Service Inventory", null, siValue)));
	}

	private Set<String> extractSiFeatureValues(List<SIFeature> siFeatures) {
		if (siFeatures == null)
			return new HashSet<>();
		return siFeatures.stream().filter(f -> f.getFeatureSpecificationCharacteristic() != null)
				.flatMap(f -> f.getFeatureSpecificationCharacteristic().stream())
				.filter(c -> c != null && c.getFeatureValue() != null && c.getMtasCode() != null)
                .filter(c -> !"Y".equalsIgnoreCase(c.getIsLCC()))
				.map(SIFeatureCharacteristic::getFeatureValue).collect(Collectors.toSet());
	}

	private Set<String> extractRfsFeatureValues(List<RfsFeaturesDTO> list) {
		if (list == null)
			return new HashSet<>();
		return list.stream().filter(rfs -> rfs.getFeatureSpecificationCharacteristic() != null)
				.flatMap(rfs -> rfs.getFeatureSpecificationCharacteristic().stream())
				.filter(spec -> spec.getMtasCode() != null && !spec.getMtasCode().isBlank())
				.filter(spec -> !"Y".equalsIgnoreCase(spec.getIsLCC()))
				.map(FeatureSpecificationCharacteristicDTO::getFeatureValue).filter(Objects::nonNull)
				.collect(Collectors.toSet());
	}

	private void performBundleComparison(List<RfsFeaturesDTO> rfsFeatures, List<SIFeature> siFeatures,  List<BIFeature> biFeatures,
			List<SIBucket> siBuckets, Map<String, String> ocsBundleMapping, List<AuditMessage> auditMessages) {

		// Extract unique OCS bundle IDs from DCM (RFS features)
		Set<String> dcmBundleIds = extractDcmBundleIds(rfsFeatures);

		// Extract base bundle from DCM
		String dcmBaseBundle = extractDcmBaseBundle(rfsFeatures);

		// Extract OCS bundle IDs from BI features
		Set<String> ocsBundleIds = extractOcsBundleIds(biFeatures);

        Set<String> siBundleIds = extractSIBundleIds(siFeatures,ocsBundleIds,ocsBundleMapping);

		// Extract SI bundle names from SI buckets
		Set<String> siBundleNames = extractSiBundleNames(siBuckets);

		log.debug("Bundle comparison - DCM bundles: {}, OCS bundles: {}, SI bundles: {}, Base bundle: {}",
				dcmBundleIds, ocsBundleIds, siBundleNames, dcmBaseBundle);

		// 1. Base bundle from DCM must match OCS
		if (dcmBaseBundle != null && !dcmBaseBundle.isEmpty()) {
			if (!ocsBundleIds.contains(dcmBaseBundle)) {
				auditMessages.add(createAuditMessage("Bundle", dcmBaseBundle,
						String.format("Base bundle from DCM not found in OCS. DCM base bundle: %s", dcmBaseBundle),
						dcmBaseBundle, null));
			}
		}

		// 2. Additional Bundles: OCS has bundles not present in SI or DCM
		ocsBundleIds.stream()
				.filter(ocsBundle -> !siBundleIds.contains(ocsBundle) )
				.forEach(ocsBundle -> auditMessages.add(createAuditMessage("Additional Bundles", ocsBundle,
						String.format("Additional bundle in OCS not found in SI: %s", ocsBundle),
						null, ocsBundle)));

		// 3. Missing Bundles: DCM bundles absent from OCS
	/*	dcmBundleIds.stream()
				.filter(dcmBundle -> !ocsBundleIds.contains(dcmBundle))
				.forEach(dcmBundle -> auditMessages.add(createAuditMessage("Missing Bundles", dcmBundle,
						String.format("Bundle from DCM is missing in OCS: %s", dcmBundle),
						dcmBundle, null))); */
/*
		// 4. Missing Bundles: DCM bundles absent from SI
		dcmBundleIds.stream()
				.filter(dcmBundle -> !siBundleNames.contains(dcmBundle))
				.forEach(dcmBundle -> auditMessages.add(createAuditMessage("Missing Bundles", dcmBundle,
						String.format("Bundle from DCM is missing in SI: %s", dcmBundle),
						dcmBundle, null))); */
	}

    private String getMTASEquivalentFromOCSBundle(String ocsBundleId, Map<String, String> ocsBundleMapping) {
        if (ocsBundleId == null || ocsBundleMapping == null || ocsBundleMapping.isEmpty()) {
            return null;
        }

        // The map contains a single entry with key "OcsBundleMapping" and value like "{SPTHR1TF224G=B01, SPTHR2TF224G=B02}"
        String mappingString = ocsBundleMapping.get(AppConstants.OCS_BUNDLE_MAPPING);
        if (mappingString == null || mappingString.isEmpty()) {
            return null;
        }

        // Remove curly braces and split by comma
        String cleanedMapping = mappingString.replace("{", "").replace("}", "").trim();
        String[] pairs = cleanedMapping.split(",\\s*");

        for (String pair : pairs) {
            String[] keyValue = pair.split("=");
            if (keyValue.length == 2) {
                String key = keyValue[0].trim();
                String value = keyValue[1].trim();
                if (key.equals(ocsBundleId)) {
                    return value; // Return the MTAS code (e.g., B01, B02, B03)
                }
            }
        }

        return null;
    }

    private Set<String> extractSIBundleIds(List<SIFeature> siFeatures, Set<String> ocsBundleIds, Map<String, String> ocsBundleMapping) {
        if (siFeatures == null || ocsBundleIds == null || ocsBundleMapping == null) {
            return new HashSet<>();
        }

        return ocsBundleIds.stream()
                .filter(ocsBundleId -> {
                    // Get MTAS code for this OCS bundle ID
                    String mtasCode = getMTASEquivalentFromOCSBundle(ocsBundleId, ocsBundleMapping);
                    if (mtasCode == null) {
                        return false;
                    }

                    // Check if any SI feature has this MTAS code
                    return siFeatures.stream()
                            .filter(siFeature -> siFeature.getFeatureSpecificationCharacteristic() != null)
                            .flatMap(siFeature -> siFeature.getFeatureSpecificationCharacteristic().stream())
                            .anyMatch(characteristic -> characteristic != null
                                    && characteristic.getMtasCode() != null
                                    && characteristic.getMtasCode().equalsIgnoreCase(mtasCode));
                })
                .collect(Collectors.toSet());
    }

	private Set<String> extractDcmBundleIds(List<RfsFeaturesDTO> rfsFeatures) {
		if (rfsFeatures == null) return new HashSet<>();
		return rfsFeatures.stream()
				.filter(rfs -> rfs.getFeatureSpecificationCharacteristic() != null)
				.flatMap(rfs -> rfs.getFeatureSpecificationCharacteristic().stream())
				.filter(spec -> spec.getOcsBundleId() != null && !spec.getOcsBundleId().isBlank())
				.map(FeatureSpecificationCharacteristicDTO::getOcsBundleId)
				.collect(Collectors.toSet());
	}

	private String extractDcmBaseBundle(List<RfsFeaturesDTO> rfsFeatures) {
		if (rfsFeatures == null) return null;
		return rfsFeatures.stream()
				.filter(rfs -> rfs.getFeatureSpecificationCharacteristic() != null)
				.flatMap(rfs -> rfs.getFeatureSpecificationCharacteristic().stream())
				.filter(spec -> spec.getBaseBundle() != null && !spec.getBaseBundle().isBlank()
						&& spec.getOcsBundleId() != null && !spec.getOcsBundleId().isBlank())
				.map(FeatureSpecificationCharacteristicDTO::getOcsBundleId)
				.findFirst()
				.orElse(null);
	}

	private Set<String> extractOcsBundleIds(List<BIFeature> biFeatures) {
		if (biFeatures == null) return new HashSet<>();
		return biFeatures.stream()
				.filter(f -> f.getId() != null && !f.getId().isBlank())
				.map(BIFeature::getId)
				.collect(Collectors.toSet());
	}

	private Set<String> extractSiBundleNames(List<SIBucket> siBuckets) {
		if (siBuckets == null) return new HashSet<>();
		return siBuckets.stream()
				.filter(b -> b.getBucketName() != null && !b.getBucketName().isBlank())
				.map(SIBucket::getBucketName)
				.collect(Collectors.toSet());
	}

	private void performServiceEndDateComparison(String soServiceEndDate, String siServiceEndDate,
			String biServiceEndDate, List<AuditMessage> auditMessages) {

		if (soServiceEndDate == null || soServiceEndDate.isBlank()) {
			return;
		}

		String normalizedSoDate = normalizeDateFormat(soServiceEndDate);

		if (siServiceEndDate != null && !siServiceEndDate.isBlank()) {
			String normalizedSiDate = normalizeDateFormat(siServiceEndDate);
			if (!normalizedSoDate.equals(normalizedSiDate)) {
				auditMessages.add(createAuditMessage("Service End Date", "SI",
						String.format("Service end date mismatch. ServiceOrder:%s, ServiceInventory:%s",
								soServiceEndDate, siServiceEndDate),
						normalizedSoDate, normalizedSiDate));
			}
		}

		if (biServiceEndDate != null && !biServiceEndDate.isBlank()) {
			String normalizedBiDate = normalizeDateFormat(biServiceEndDate);
			if (!normalizedSoDate.equals(normalizedBiDate)) {
				auditMessages.add(createAuditMessage("Service End Date", "OCS",
						String.format("Service end date mismatch. ServiceOrder:%s, OCS:%s",
								soServiceEndDate, biServiceEndDate),
						normalizedSoDate, normalizedBiDate));
			}
		}
	}


	private void performIccidComparison(String qmdnIccid, String simSerialNumber, List<AuditMessage> auditMessages) {
		if (qmdnIccid != null && simSerialNumber != null) {
			if (!qmdnIccid.equals(simSerialNumber)) {
				AuditMessage message = createAuditMessage("ICCID", "",
						String.format("ICCID Mismatch. QMDN:%s, ProductInventory:%s", qmdnIccid, simSerialNumber), simSerialNumber, qmdnIccid);
				auditMessages.add(message);
			}
		}
	}

	private void performLifecycleStatusComparison(String siStatus, String biLifeCycleState,
			List<AuditMessage> auditMessages, AuditValidationRequestData requestData) {
		
		// Skip lifecycle status comparison for FWA devices if feature flag is enabled
		if (featureFlagHelper.isFwaSkipAuditChecksFFlag() && isFwaDevice(requestData)) {
			log.info("⊘ Lifecycle Status Comparison Skipped: Skipped for FWA device - FwaNodeId: {}, FwaReservationId: {}", 
					requestData.getFwaNodeId(), requestData.getFwaReservationId());
			return;
		}
		
		if (siStatus != null && biLifeCycleState != null) {
			if (!siStatus.equalsIgnoreCase(biLifeCycleState)) {
				AuditMessage message = createAuditMessage("Lifecycle Status", "",
						String.format("Lifecycle status mismatch. ServiceInventory:%s, BundleInquiry:%s", siStatus,
								biLifeCycleState),
						siStatus, biLifeCycleState);
				auditMessages.add(message);
			}
		}
	}

	private AuditMessage createAuditMessage(String group, String id, String message, String expected, String actual) {
		AuditMessage auditMessage = new AuditMessage();
		auditMessage.setAuditGroup(group);
		auditMessage.setId(id);
		auditMessage.setMessage(message);
		auditMessage.setExpected(expected);
		auditMessage.setActual(actual);
		return auditMessage;
	}

	private List<FailureType> groupAuditMessagesByType(List<AuditMessage> auditMessages) {
		Map<String, List<AuditMessage>> groupedMessages = auditMessages.stream()
				.collect(Collectors.groupingBy(AuditMessage::getAuditGroup));

		List<FailureType> failureTypes = new ArrayList<>();
		
		for (Map.Entry<String, List<AuditMessage>> entry : groupedMessages.entrySet()) {
			FailureType failureType = new FailureType();
			failureType.setFailureType(entry.getKey());
			
			List<FailureData> failureDataList = entry.getValue().stream()
					.map(msg -> {
						FailureData failureData = new FailureData();
						failureData.setId(msg.getId() != null ? msg.getId() : "");
						
						failureData.setFailureMessage(msg.getMessage());
						failureData.setExpected(msg.getExpected());
						failureData.setActual(msg.getActual());
						
						return failureData;
					})
					.collect(Collectors.toList());
			
			failureType.setFailureData(failureDataList);
			failureTypes.add(failureType);
		}
		
		return failureTypes;
	}

	private List<FailureType> buildAuditReportWithAllTiles(List<AuditMessage> auditMessages) {
        List<FailureType> report = new ArrayList<>();
        if (null != auditMessages && !auditMessages.isEmpty()) {
            Map<String, List<AuditMessage>> groupedMessages = auditMessages.stream()
                    .collect(Collectors.groupingBy(AuditMessage::getAuditGroup));


            List<String> defaultTiles = getDefaultAuditTiles();

            for (String tile : defaultTiles) {
                FailureType failureType = new FailureType();
                failureType.setFailureType(tile);

                List<AuditMessage> tileMessages = groupedMessages.remove(tile);
                if (tileMessages == null || tileMessages.isEmpty()) {
                    failureType.setFailureData(new ArrayList<>());
                } else {
                    failureType.setFailureData(mapAuditMessagesToFailureData(tileMessages));
                }
                report.add(failureType);
            }

            for (Map.Entry<String, List<AuditMessage>> entry : groupedMessages.entrySet()) {
                List<FailureData> failedData = mapAuditMessagesToFailureData(entry.getValue());
                if (null != failedData && !failedData.isEmpty()) {
                    FailureType failureType = new FailureType();
                    failureType.setFailureType(entry.getKey());
                    failureType.setFailureData(failedData);
                    report.add(failureType);
                }
            }
        }
		return report;
	}

	private List<FailureData> mapAuditMessagesToFailureData(List<AuditMessage> messages) {
		return messages.stream().map(msg -> {
			FailureData failureData = new FailureData();
			failureData.setId(msg.getId() != null ? msg.getId() : "");
			failureData.setFailureMessage(msg.getMessage());
			failureData.setExpected(msg.getExpected());
			failureData.setActual(msg.getActual());
			return failureData;
		}).collect(Collectors.toList());
	}

	private List<String> getDefaultAuditTiles() {
		List<String> tiles = new ArrayList<>();
		tiles.add("Bucket value");
		tiles.add("Bucket end date");
		tiles.add("Bucket missing in OCS");
		tiles.add("Additional Buckets");
		tiles.add("Missing Buckets");
		tiles.add("Feature Missing");
		tiles.add("Additional Feature");
		tiles.add("Bundle");
		tiles.add("Additional Bundles");
		tiles.add("Missing Bundles");
		tiles.add("Service End Date");
		tiles.add("ICCID");
		tiles.add("Lifecycle Status");
		return tiles;
	}

	private double convertToBaseUnit(double value, String unit) {
		if (unit == null) {
			return value;
		}

		switch (unit.toLowerCase()) {
		case "kb":
			return value;
		case "mb":
			return value * 1024.0;
		case "gb":
			return value * 1024.0 * 1024.0;
		case "tb":
			return value * 1024.0 * 1024.0 * 1024.0;
		case "b":
		case "bytes":
			return value / 1024.0;
		default:
			return value;
		}
	}

	private String normalizeDateFormat(String dateStr) {
		if (dateStr == null || dateStr.trim().isEmpty()) {
			return "";
		}
		try {
			String trimmedDate = dateStr.trim();

			// Handle format: 2026-08-23-09.25.14.549 (YYYY-MM-dd-HH.mm.ss.SSS)
			if (trimmedDate.matches("\\d{4}-\\d{2}-\\d{2}-\\d{2}\\.\\d{2}\\.\\d{2}\\.\\d{3}")) {
				return trimmedDate.substring(0, 10); // Extract YYYY-MM-dd
			}

			// Handle format: 2026-08-23T09:25:14Z (ISO 8601 with time)
			if (trimmedDate.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z?")) {
				return trimmedDate.substring(0, 10); // Extract YYYY-MM-dd
			}

			// Handle format: MMddYYYY or MddYYYY (7 or 8 digits)
			if (trimmedDate.matches("\\d{7,8}")) {
				return parseMddyyyyOrMmddyyyy(trimmedDate);
			}

			// Handle format: YYYY-MM-dd (already in target format)
			if (trimmedDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
				return trimmedDate;
			}

			// Handle format: MM/dd/YYYY or M/dd/YYYY or MM/d/YYYY or M/d/YYYY
			if (trimmedDate.matches("\\d{1,2}/\\d{1,2}/\\d{4}")) {
				String[] parts = trimmedDate.split("/");
				String month = parts[0].length() == 1 ? "0" + parts[0] : parts[0];
				String day = parts[1].length() == 1 ? "0" + parts[1] : parts[1];
				return parts[2] + "-" + month + "-" + day; // Convert to YYYY-MM-dd
			}

			log.warn("Unrecognized date format: {}", dateStr);
			return dateStr;
		} catch (Exception e) {
			log.warn("Error normalizing date format: {}", dateStr, e);
			return dateStr;
		}
	}

	/**
	 * Parse date format MddYYYY or MMddYYYY
	 * Examples: 8232026 (MMddYYYY), 832026 (MddYYYY)
	 */
	private String parseMddyyyyOrMmddyyyy(String dateStr) {
		int length = dateStr.length();
		String year = dateStr.substring(length - 4); // Last 4 digits are year
		String monthDay = dateStr.substring(0, length - 4); // Remaining digits are month and day

		String month;
		String day;

		// Try to parse as MMdd first (most common case)
		if (monthDay.length() == 4) {
			month = monthDay.substring(0, 2);
			day = monthDay.substring(2, 4);

			// Validate month range (01-12)
			int monthInt = Integer.parseInt(month);
			if (monthInt >= 1 && monthInt <= 12) {
				return year + "-" + month + "-" + day;
			}
		}

		// Try to parse as Mdd (single digit month)
		if (monthDay.length() == 3) {
			month = "0" + monthDay.substring(0, 1);
			day = monthDay.substring(1, 3);
			return year + "-" + month + "-" + day;
		}

		// Fallback: assume MMdd format
		if (monthDay.length() == 4) {
			month = monthDay.substring(0, 2);
			day = monthDay.substring(2, 4);
			return year + "-" + month + "-" + day;
		}

		log.warn("Unable to parse date format: {}", dateStr);
		return dateStr;
	}

	private double parseDouble(String value) {
		if (value == null) {return 0.0;}
		try {
			return Double.parseDouble(value);
		} catch (NumberFormatException e) {
			log.error("Error parsing double value: {}", value, e);
			return 0.0;
		}
	}

	/**
	 * Check if device is FWA (Fixed Wireless Access)
	 * A device is classified as FWA if either FwaNodeId or FwaReservationId is present and non-empty
	 *
	 * @param requestData Audit validation request data containing FWA fields
	 * @return true if device is FWA, false otherwise
	 */
	private boolean isFwaDevice(AuditValidationRequestData requestData) {
		if (requestData == null) {
			return false;
		}
		
		String fwaNodeId = requestData.getFwaNodeId();
		String fwaReservationId = requestData.getFwaReservationId();
		
		boolean isFwa = (fwaNodeId != null && !fwaNodeId.trim().isEmpty()) ||
		                (fwaReservationId != null && !fwaReservationId.trim().isEmpty());
		
		if (isFwa) {
			log.debug("Device detected as FWA - FwaNodeId: {}, FwaReservationId: {}", fwaNodeId, fwaReservationId);
		}
		
		return isFwa;
	}

}