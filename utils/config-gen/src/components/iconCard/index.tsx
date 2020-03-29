import React from 'react';
import SourceIcon from '../icons/sourceIcon';
import DestinationIcon from '../icons/destinationIcon';
import { Container, AbsoluteContaier } from './styles';
import { LabelMedium } from '../common/typography';
import Svg from '@svg/index';
import theme from '@css/theme';

interface IProps {
  name: string;
  title: string;
  type: string;
  selected: boolean;
  selectionMode: string;
  onClick: () => void;
}

const IconCard: React.FC<IProps> = ({
  name,
  title,
  type,
  selected,
  selectionMode,
  onClick,
}) => {
  return (
    <Container className={selected ? 'active' : ''} onClick={onClick}>
      {type === 'source' && (
        <SourceIcon
          source={name}
          height={theme.iconSize.large}
          width={theme.iconSize.large}
        />
      )}
      {type === 'destination' && (
        <DestinationIcon
          destination={name}
          height={theme.iconSize.large}
          width={theme.iconSize.large}
        />
      )}
      {selected && selectionMode === 'multi' && (
        <AbsoluteContaier>
          <Svg name="selected" />
        </AbsoluteContaier>
      )}
      <div className="p-t-sm">
        <LabelMedium color={theme.color.black}>{title}</LabelMedium>
      </div>
    </Container>
  );
};

export default IconCard;
