package com.lacus.utils;

import com.alibaba.fastjson2.JSONObject;
import com.lacus.common.exception.ApiException;
import com.lacus.common.exception.error.ErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.utils.URIBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.net.URI;

@Component
@Slf4j
public class RestUtil {

    @Autowired
    private RestTemplate restTemplate;

    public String getForString(String url) {
        try {
            URI uri = new URIBuilder(url).build();
            return restTemplate.getForObject(uri, String.class);
        } catch (Exception e) {
            throw new ApiException(ErrorCode.Internal.UNKNOWN_ERROR, "rest api请求失败: " + e.getMessage());
        }
    }

    public Object getForObject(String url) {
        try {
            URI uri = new URIBuilder(url).build();
            return restTemplate.getForObject(uri, Object.class);
        } catch (Exception e) {
            throw new ApiException(ErrorCode.Internal.UNKNOWN_ERROR, "rest api请求失败: " + e.getMessage());
        }
    }

    public JSONObject getForJsonObject(String url) {
        try {
            URI uri = new URIBuilder(url).build();
            return restTemplate.getForObject(uri, JSONObject.class);
        } catch (Exception e) {
            log.error("rest api请求失败:", e);
            throw new ApiException(ErrorCode.Internal.UNKNOWN_ERROR, "rest api请求失败");
        }
    }

    public JSONObject postForJsonObject(String url, JSONObject param) {
        try {
            URI uri = new URIBuilder(url).build();
            return restTemplate.postForObject(uri, param, JSONObject.class);
        } catch (Exception e) {
            log.error("rest api请求失败:", e);
            throw new ApiException(ErrorCode.Internal.UNKNOWN_ERROR, "rest api请求失败");
        }
    }

    public <T> ResponseEntity<T> exchangeGet(String url, HttpHeaders headers, Class<T> responseType, Object... uriVariables) {
        try {
            HttpEntity<JSONObject> httpEntity = new HttpEntity<>(headers);
            return restTemplate.exchange(url, HttpMethod.GET, httpEntity, responseType, uriVariables);
        } catch (Exception ex) {
            log.error("rest api请求失败:", ex);
            throw new ApiException(ErrorCode.Internal.UNKNOWN_ERROR, "rest api请求失败");
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> ResponseEntity<T> postForEntity(String url, HttpHeaders headers, Object param, Class<T> responseType) {
        try {
            HttpEntity httpEntity = new HttpEntity(param, headers);
            return restTemplate.postForEntity(url, httpEntity, responseType);
        } catch (Exception ex) {
            log.error("rest api请求失败:", ex);
            throw new ApiException(ErrorCode.Internal.UNKNOWN_ERROR, "rest api请求失败");
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> ResponseEntity<T> exchangePut(String url, HttpHeaders headers, Object param, Class<T> responseType) {
        try {
            HttpEntity httpEntity = new HttpEntity(param, headers);
            return restTemplate.exchange(url, HttpMethod.PUT, httpEntity, responseType);
        } catch (Exception ex) {
            log.error("rest api请求失败:", ex);
            throw new ApiException(ErrorCode.Internal.UNKNOWN_ERROR, "rest api请求失败");
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> ResponseEntity<T> exchangeDelete(String url, HttpHeaders headers, Class<T> responseType) {
        try {
            HttpEntity httpEntity = new HttpEntity(headers);
            return restTemplate.exchange(url, HttpMethod.DELETE, httpEntity, responseType);
        } catch (Exception ex) {
            log.error("rest api请求失败:", ex);
            throw new ApiException(ErrorCode.Internal.UNKNOWN_ERROR, "rest api请求失败");
        }
    }
}
