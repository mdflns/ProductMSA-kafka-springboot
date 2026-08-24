package com.nazoogomtang.ws.products.service;

import com.nazoogomtang.ws.products.rest.CreateProductRestModel;

public interface ProductService {
    public String createProduct(CreateProductRestModel productRestModel);
}
