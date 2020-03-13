import * as React from 'react';
import styled from 'styled-components';
import is from 'typescript-styled-is';

import { Flex } from './misc';
import DestinationIcon from '@components/icons/destinationIcon';
import SourceIcon from '@components/icons/sourceIcon';

const DottedCircleOutline = styled(Flex)`
  display: flex;
  flex-direction: row;
  justify-content: center;
  align-items: center;
  width: 50px;
  height: 50px;
  border-radius: 50%;
  border: 2px dashed ${({ theme }) => theme.color.grey100};
  ${is('active')`
    border: 2px dashed ${({ theme }) => theme.color.primary200};
    `};
  img {
    max-width: 40px;
    max-height: 40px;
  }
`;

const SolidInnerCircle = styled.div`
  width: 28px;
  height: 28px;
  border-radius: 50%;
  margin: auto;
  background: ${({ theme }) => theme.color.primary200};
  opacity: 0.3;
`;

interface IDottedCircleProps {
  solid?: boolean;
  iconName?: string;
  destination?: boolean;
}

export const DottedCircle: React.FC<IDottedCircleProps> = (
  props: IDottedCircleProps,
) => {
  if (props.iconName) {
    return (
      <DottedCircleOutline>
        {props.destination ? (
          <DestinationIcon destination={`${props.iconName}`} />
        ) : (
          <SourceIcon source={`${props.iconName}`} />
        )}
      </DottedCircleOutline>
    );
  } else {
    return (
      <DottedCircleOutline active={props.solid}>
        {props.solid && <SolidInnerCircle />}
      </DottedCircleOutline>
    );
  }
};

const SolidCirlceOutline = styled(Flex)<any>`
  display: flex;
  flex-direction: row;
  justify-content: center;
  align-items: center;
  width: 50px;
  height: 50px;
  border-radius: 50%;
  border: 2px solid ${({ theme }) => theme.color.primary10};
  background: ${({ theme }) => theme.color.white};
  position: relative;
  left: ${props =>
    props.listIndex !== undefined ? -14 * (props.listIndex + 1) + 'px' : 0};
  img {
    max-width: 40px;
    max-height: 40px;
  }
`;

interface IIconCirlceProps {
  name: string;
  listIndex?: number;
  destination?: boolean;
}

export const IconCircle: React.FC<IIconCirlceProps> = (
  props: IIconCirlceProps,
) => {
  return (
    <SolidCirlceOutline listIndex={props.listIndex}>
      {props.destination ? (
        <DestinationIcon destination={`${props.name}`} />
      ) : (
        <SourceIcon source={`${props.name}`} />
      )}
    </SolidCirlceOutline>
  );
};
