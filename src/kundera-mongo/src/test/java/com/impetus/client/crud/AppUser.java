/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.crud;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "User", schema = "KunderaExamples@mongoTest")
public class AppUser
{
    static enum PhoneKind
    {
        PERSONAL,
        WORK,
        MOBILE
    }
    
    static enum PhonePriority
    {
        ONLY_EMERGENCY,
        LOW,
        HIGH
    }
    
    @Id
    private String id;

    @Column
    private List<String> tags;

    @Column
    private Map<String, String> propertyKeys;

    @Column
    private Set<String> nickNames;

    @Column
    protected List<String> friendList;

    @Embedded
    private PhoneDirectory phoneDirectory;
    
    @Column
    private Map<PhoneKind, String> phones;
    
    @Column
    private Map<PhoneKind, PhonePriority> phonePriorities;

    public AppUser()
    {
        tags = new LinkedList<String>();
        propertyKeys = new HashMap<String, String>();
        nickNames = new HashSet<String>();
        friendList = new LinkedList<String>();
        phones = new HashMap<PhoneKind, String>();
        phonePriorities = new HashMap<PhoneKind, PhonePriority>();
        tags.add("yo");
        propertyKeys.put("kk", "Kuldeep");
        nickNames.add("kk");
        friendList.add("xamry");
        friendList.add("mevivs");

        phones.put(PhoneKind.PERSONAL, "+1-123-456-7857");
        phones.put(PhoneKind.MOBILE, "+5-666-777-9998");

        phonePriorities.put(PhoneKind.PERSONAL, PhonePriority.ONLY_EMERGENCY);
        phonePriorities.put(PhoneKind.MOBILE, PhonePriority.HIGH);
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public List<String> getTags()
    {
        return tags;
    }

    public Map<String, String> getPropertyKeys()
    {
        return propertyKeys;
    }

    public Set<String> getNickName()
    {
        return nickNames;
    }

    public List<String> getFriendList()
    {
        return friendList;
    }

    public PhoneDirectory getPhoneDirectory()
    {
        return phoneDirectory;
    }

    public void setPropertyContainer(PhoneDirectory propertyContainer)
    {
        this.phoneDirectory = propertyContainer;
    }

    public Map<PhoneKind, String> getPhones()
    {
        return phones;
    }

    public Map<PhoneKind, PhonePriority> getPhonePriorities()
    {
        return phonePriorities;
    }
}
